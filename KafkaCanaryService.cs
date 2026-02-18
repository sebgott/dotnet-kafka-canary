using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;
using dotnet_kafka_canary.Domain.Metrics;

namespace dotnet_kafka_canary;

public class KafkaCanaryService : BackgroundService
{
    private readonly ILogger<KafkaCanaryService> _logger;

    private readonly string _topic;
    private readonly string _instanceId = Guid.NewGuid().ToString();

    private readonly IProducer<string, string> _producer;
    private readonly IConsumer<string, string> _consumer;

    private readonly ConcurrentDictionary<Guid, DateTime> _inflight = new();

    public static DateTime LastSuccessUtc { get; private set; } = DateTime.MinValue;

    public KafkaCanaryService(ILogger<KafkaCanaryService> logger, IConfiguration config)
    {
        _logger = logger;

        _topic = config["KAFKA:TOPIC"] ?? "kafka-canary";

        var bootstrap = config["KAFKA:BOOTSTRAPSERVERS"] ?? "localhost:9092";
        var sslCaLocation = config["KAFKA:SSLCALOCATION"];
        var sslKeystoreLocation = config["KAFKA:SSLKEYSTORELOCATION"];
        var sslKeystorePassword = config["KAFKA:SSLKEYSTOREPASSWORD"];
        
        _logger.LogDebug("SslCaLocation: {Location} \n" +
                               "SslKeystoreLocation: {KeyLocation} \n",
            sslCaLocation, sslKeystoreLocation);

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            SecurityProtocol = SecurityProtocol.Ssl,
            SslCaLocation = sslCaLocation,
            SslKeystoreLocation = sslKeystoreLocation,
            SslKeystorePassword = sslKeystorePassword,
            Acks = Acks.All,
            EnableIdempotence = true
        };

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            SecurityProtocol = SecurityProtocol.Ssl,
            SslCaLocation = sslCaLocation,
            SslKeystoreLocation = sslKeystoreLocation,
            SslKeystorePassword = sslKeystorePassword,
            GroupId = "kafka-canary-group",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Latest
        };

        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
        _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        _consumer.Subscribe(_topic);
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

            var json = JsonSerializer.Serialize(message);

            try
            {
                await _producer.ProduceAsync(_topic, new Message<string, string>
                {
                    Key = _instanceId,
                    Value = json
                }, token);

                _inflight[message.MessageId] = message.ProducedAtUtc;
                PrometheusMetrics.MessagesProduced.Inc();

                _logger.LogInformation("Produced {MessageId}", message.MessageId);
            }
            catch (Exception ex)
            {
                PrometheusMetrics.Failures.Inc();
                _logger.LogError(ex, "Produce failed");
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
                try
                {
                    var result = _consumer.Consume(token);

                    var message = JsonSerializer.Deserialize<CanaryMessage>(result.Message.Value);

                    if (message == null)
                        continue;

                    if (_inflight.TryRemove(message.MessageId, out var producedAt))
                    {
                        var latency = DateTime.UtcNow - producedAt;
                        PrometheusMetrics.MessagesConsumed.Inc();
                        PrometheusMetrics.RoundtripLatency.Observe(latency.TotalMilliseconds);

                        LastSuccessUtc = DateTime.UtcNow;

                        _logger.LogInformation(
                            "Roundtrip OK {MessageId} in {Latency} ms",
                            message.MessageId,
                            latency.TotalMilliseconds);
                    }

                    _consumer.Commit(result);
                }
                catch (ConsumeException ex)
                {
                    PrometheusMetrics.Failures.Inc();
                    _logger.LogError(ex, "Consume error");
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    PrometheusMetrics.Failures.Inc();
                    _logger.LogError(ex, "Unexpected consume error");
                }
            }
        }, token);
    }

    public override void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
        _producer.Dispose();
        base.Dispose();
    }
}