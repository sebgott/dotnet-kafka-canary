using System.Collections.Concurrent;
using System.Diagnostics;
using dotnet_kafka_canary.Domain.CloudEvents;
using dotnet_kafka_canary.Domain.Metrics;
using dotnet_kafka_canary.Domain.Tracing;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace dotnet_kafka_canary;

public class KafkaCanaryService : BackgroundService
{
    private readonly ILogger<KafkaCanaryService> _logger;

    private readonly string _topic;
    private readonly string _groupId;
    private readonly string _instanceId = Guid.NewGuid().ToString();

    private readonly IProducer<string?, byte[]> _producer;
    private readonly IConsumer<string?, byte[]> _consumer;

    private readonly ConcurrentDictionary<Guid, DateTime> _inflight = new();

    public static DateTime LastSuccessUtc { get; private set; } = DateTime.MinValue;

    public KafkaCanaryService(
        ILogger<KafkaCanaryService> logger,
        IOptions<KafkaOptions> options,
        KafkaClients clients)
    {
        _logger = logger;
        _topic = options.Value.Topic;
        _groupId = options.Value.GroupId;
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
            var message = new CanaryMessage(
                _instanceId,
                Guid.NewGuid(),
                DateTime.UtcNow);

            using (var activity = KafkaCanaryTelemetry.StartProduceActivity(_topic, _instanceId, message.MessageId))
            {
                _inflight[message.MessageId] = message.ProducedAtUtc;

                try
                {
                    var kafkaMessage = CanaryCloudEvents.ToKafkaMessage(message, activity);
                    KafkaCanaryTelemetry.Inject(activity, kafkaMessage.Headers);

                    var result = await _producer.ProduceAsync(_topic, kafkaMessage, token);

                    activity?.SetTag("messaging.destination.partition.id", result.Partition.Value.ToString());
                    activity?.SetTag("messaging.kafka.offset", result.Offset.Value);

                    PrometheusMetrics.MessagesProduced.Inc();

                    _logger.LogInformation("Produced {MessageId}", message.MessageId);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    _inflight.TryRemove(message.MessageId, out _);
                    break;
                }
                catch (ProduceException<string?, byte[]> ex)
                {
                    _inflight.TryRemove(message.MessageId, out _);
                    PrometheusMetrics.Failures.Inc();
                    RecordError(activity, ex);
                    _logger.LogError("Error producing message 'ProduceException': {Message}", ex.Message);
                }
                catch (Exception ex)
                {
                    _inflight.TryRemove(message.MessageId, out _);
                    PrometheusMetrics.Failures.Inc();
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
                    PrometheusMetrics.Failures.Inc();
                    _logger.LogError(ex, "Consume error");
                    continue;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    PrometheusMetrics.Failures.Inc();
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
                    PrometheusMetrics.Failures.Inc();
                    RecordError(activity, ex);
                    _logger.LogError(ex, "Unexpected consume error");
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

        if (_inflight.TryRemove(message.MessageId, out var producedAt))
        {
            var latency = DateTime.UtcNow - producedAt;

            PrometheusMetrics.MessagesConsumed.Inc();
            PrometheusMetrics.RoundtripLatency.Observe(latency.TotalMilliseconds);

            LastSuccessUtc = DateTime.UtcNow;

            activity?.SetTag("kafka_canary.roundtrip_ms", latency.TotalMilliseconds);

            _logger.LogInformation(
                "Roundtrip OK {MessageId} in {Latency} ms",
                message.MessageId,
                latency.TotalMilliseconds);
        }
    }

    private static void RecordError(Activity? activity, Exception ex)
    {
        activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
        activity?.AddException(ex);
    }
}
