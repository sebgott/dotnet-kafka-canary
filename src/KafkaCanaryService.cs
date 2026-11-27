using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
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
    private readonly TimeSpan _produceInterval;
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
        _produceInterval = TimeSpan.FromMilliseconds(options.Value.ProduceIntervalMs);
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
        // Fixed-rate schedule: the interval includes the time spent waiting for the broker ack.
        // Ticks missed while a produce is blocked are coalesced rather than sent in a burst.
        using var timer = new PeriodicTimer(_produceInterval);

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
                    LogFailure(FailureStage.Produce, ex, message.MessageId.ToString(), _topic);
                }
                catch (Exception ex)
                {
                    RemoveInflight(message.MessageId);
                    PrometheusMetrics.Failures.WithLabels(FailureStage.Produce).Inc();
                    RecordError(activity, ex);
                    LogFailure(FailureStage.Produce, ex, message.MessageId.ToString(), _topic);
                }
            }

            await timer.WaitForNextTickAsync(token);
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
                    var record = ex.ConsumerRecord;
                    LogFailure(FailureStage.Consume, ex, GetMessageId(record?.Message?.Headers),
                        record?.Topic ?? _topic, record?.Partition.Value, record?.Offset.Value);
                    continue;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    PrometheusMetrics.Failures.WithLabels(FailureStage.Consume).Inc();
                    LogFailure(FailureStage.Consume, ex, topic: _topic);
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
                    LogFailure(FailureStage.Process, ex, GetMessageId(result.Message?.Headers),
                        result.Topic, result.Partition.Value, result.Offset.Value);
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

    private void LogFailure(
        string stage, Exception exception, string? messageId = null, string? topic = null,
        int? partition = null, long? offset = null)
    {
        _logger.LogError(exception,
            "Canary failure: stage={Stage} message_id={MessageId} topic={Topic} partition={Partition} offset={Offset} error_type={ErrorType} error_code={ErrorCode} reason={Reason}",
            stage, SingleLine(messageId), SingleLine(topic), partition is >= 0 ? partition.ToString() : "unknown",
            offset is >= 0 ? offset.ToString() : "unknown", exception.GetType().Name,
            exception is KafkaException kafkaException ? kafkaException.Error.Code.ToString() : "unknown",
            SingleLine(exception.Message));
    }

    private static string? GetMessageId(Headers? headers) =>
        headers is not null && headers.TryGetLastBytes("ce_id", out var value) && value is not null
            ? Encoding.UTF8.GetString(value)
            : null;

    private static string SingleLine(string? value) =>
        string.IsNullOrEmpty(value) ? "unknown" : value.Replace('\r', ' ').Replace('\n', ' ');

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
