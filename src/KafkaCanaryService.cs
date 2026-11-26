using System.Collections.Concurrent;
using System.Diagnostics;
using dotnet_kafka_canary.Domain.CloudEvents;
using dotnet_kafka_canary.Domain.Metrics;
using dotnet_kafka_canary.Domain.Tracing;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Prometheus;

namespace dotnet_kafka_canary;

public class KafkaCanaryService : BackgroundService
{
    private readonly ILogger<KafkaCanaryService> _logger;

    private readonly string _topic;
    private readonly string _groupId;
    private readonly TimeSpan _consumeTimeout;
    private readonly string _instanceId = Guid.NewGuid().ToString();

    private readonly IProducer<string?, byte[]> _producer;
    private readonly IConsumer<string?, byte[]> _consumer;

    private readonly ConcurrentDictionary<Guid, InflightMessage> _inflight = new();

    public static DateTime LastSuccessUtc { get; private set; } = DateTime.MinValue;

    public KafkaCanaryService(
        ILogger<KafkaCanaryService> logger,
        IOptions<KafkaOptions> options,
        KafkaClients clients)
    {
        _logger = logger;
        _topic = options.Value.Topic;
        _groupId = options.Value.GroupId;
        _consumeTimeout = TimeSpan.FromMilliseconds(options.Value.ConsumeTimeoutMs);
        _producer = clients.Producer;
        _consumer = clients.Consumer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var producerTask = RunProducerLoop(stoppingToken);
        var consumerTask = RunConsumerLoop(stoppingToken);

        await Task.WhenAll(producerTask, consumerTask);
    }

    private async Task RunProducerLoop(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            ExpireUnconsumedMessages();

            var message = new CanaryMessage(
                _instanceId,
                Guid.NewGuid(),
                DateTime.UtcNow);

            using (var activity = KafkaCanaryTelemetry.StartProduceActivity(_topic, _instanceId, message.MessageId))
            {
                var inflight = new InflightMessage(Stopwatch.GetTimestamp());
                _inflight[message.MessageId] = inflight;
                UpdateInflightGauge();

                try
                {
                    var kafkaMessage = CanaryCloudEvents.ToKafkaMessage(message, activity);
                    KafkaCanaryTelemetry.Inject(activity, kafkaMessage.Headers);

                    var result = await _producer.ProduceAsync(_topic, kafkaMessage, token);
                    inflight.AckedTimestamp = Stopwatch.GetTimestamp();

                    activity?.SetTag("messaging.destination.partition.id", result.Partition.Value.ToString());
                    activity?.SetTag("messaging.kafka.offset", result.Offset.Value);

                    PrometheusMetrics.MessagesProduced.Inc();
                    PrometheusMetrics.ProduceLatency.Observe(
                        Stopwatch.GetElapsedTime(inflight.ProducedTimestamp, inflight.AckedTimestamp).TotalSeconds);

                    _logger.LogInformation("Produced {MessageId}", message.MessageId);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    RemoveInflight(message.MessageId);
                    break;
                }
                catch (ProduceException<string?, byte[]> ex)
                {
                    RemoveInflight(message.MessageId);
                    PrometheusMetrics.Failures.WithLabels(FailureStage.Produce).Inc();
                    RecordError(activity, ex);
                    _logger.LogError("Error producing message 'ProduceException': {Message}", ex.Message);
                }
                catch (Exception ex)
                {
                    RemoveInflight(message.MessageId);
                    PrometheusMetrics.Failures.WithLabels(FailureStage.Produce).Inc();
                    RecordError(activity, ex);
                    _logger.LogError(ex, "Produce failed");
                }
            }

            await Task.Delay(TimeSpan.FromSeconds(10), token);
        }
    }

    private Task RunConsumerLoop(CancellationToken token)
    {
        return Task.Run(() =>
        {
            while (!token.IsCancellationRequested)
            {
                ConsumeResult<string?, byte[]> result;
                try
                {
                    result = _consumer.Consume(token);
                }
                catch (ConsumeException ex)
                {
                    PrometheusMetrics.Failures.WithLabels(FailureStage.Consume).Inc();
                    _logger.LogError(ex, "Consume error");
                    continue;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    PrometheusMetrics.Failures.WithLabels(FailureStage.Consume).Inc();
                    _logger.LogError(ex, "Unexpected consume error");
                    continue;
                }

                var parentContext = KafkaCanaryTelemetry.Extract(result.Message.Headers);
                using var activity = KafkaCanaryTelemetry.StartProcessActivity(result, _groupId, parentContext);

                try
                {
                    ProcessMessage(result, activity);
                    _consumer.Commit(result);
                }
                catch (Exception ex)
                {
                    PrometheusMetrics.Failures.WithLabels(FailureStage.Process).Inc();
                    RecordError(activity, ex);
                    _logger.LogError(ex, "Message processing failed");
                }
            }
        }, token);
    }

    private void ProcessMessage(ConsumeResult<string?, byte[]> result, Activity? activity)
    {
        if (!CanaryCloudEvents.TryRead(result.Message, out var cloudEvent, out var message))
        {
            activity?.SetStatus(ActivityStatusCode.Error, "Message is not a canary CloudEvent");
            _logger.LogWarning(
                "Skipping message at {TopicPartitionOffset}: not a binary-mode canary CloudEvent",
                result.TopicPartitionOffset);
            return;
        }

        activity?.SetTag("messaging.message.id", cloudEvent.Id);
        activity?.SetTag("cloudevents.event_id", cloudEvent.Id);
        activity?.SetTag("cloudevents.event_source", cloudEvent.Source?.ToString());
        activity?.SetTag("cloudevents.event_type", cloudEvent.Type);

        if (_inflight.TryRemove(message.MessageId, out var inflight))
        {
            var latency = Stopwatch.GetElapsedTime(inflight.ProducedTimestamp);
            UpdateInflightGauge();

            PrometheusMetrics.MessagesConsumed.Inc();
            PrometheusMetrics.RoundtripLatency.Observe(latency.TotalSeconds);
            PrometheusMetrics.LastRoundtripTimestamp.SetToCurrentTimeUtc();

            LastSuccessUtc = DateTime.UtcNow;

            activity?.SetTag("kafka_canary.roundtrip_ms", latency.TotalMilliseconds);

            _logger.LogInformation(
                "Roundtrip OK {MessageId} in {Latency} ms",
                message.MessageId,
                latency.TotalMilliseconds);
        }
    }

    private void ExpireUnconsumedMessages()
    {
        foreach (var (messageId, inflight) in _inflight)
        {
            var ackedTimestamp = inflight.AckedTimestamp;
            if (ackedTimestamp == 0 || Stopwatch.GetElapsedTime(ackedTimestamp) < _consumeTimeout)
                continue;

            if (!_inflight.TryRemove(messageId, out _))
                continue;

            // With a fresh consumer group and AutoOffsetReset.Latest, messages produced before the
            // consumer is positioned are skipped; only count losses once a roundtrip has succeeded.
            if (LastSuccessUtc == DateTime.MinValue)
            {
                _logger.LogWarning(
                    "Message {MessageId} was not consumed within {Timeout} before the first roundtrip; not counted as lost",
                    messageId,
                    _consumeTimeout);
                continue;
            }

            PrometheusMetrics.MessagesLost.Inc();
            _logger.LogError(
                "Message {MessageId} was not consumed within {Timeout} after broker acknowledgement",
                messageId,
                _consumeTimeout);
        }

        UpdateInflightGauge();
    }

    private void RemoveInflight(Guid messageId)
    {
        _inflight.TryRemove(messageId, out _);
        UpdateInflightGauge();
    }

    private void UpdateInflightGauge() => PrometheusMetrics.InflightMessages.Set(_inflight.Count);

    private static void RecordError(Activity? activity, Exception ex)
    {
        activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
        activity?.AddException(ex);
    }

    private sealed class InflightMessage(long producedTimestamp)
    {
        private long _ackedTimestamp;

        public long ProducedTimestamp { get; } = producedTimestamp;

        public long AckedTimestamp
        {
            get => Volatile.Read(ref _ackedTimestamp);
            set => Volatile.Write(ref _ackedTimestamp, value);
        }
    }
}
