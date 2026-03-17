using System.Collections.Concurrent;
using System.IdentityModel.Tokens.Jwt;
using System.Text.Json;
using dotnet_kafka_canary.Domain.Metrics;
using Confluent.Kafka;

namespace dotnet_kafka_canary;

public class KafkaCanaryService : BackgroundService
{
    private readonly ILogger<KafkaCanaryService> _logger;

    private readonly string _topic;
    private readonly string _instanceId = Guid.NewGuid().ToString();

    private readonly string _clientId;
    private readonly string _clientSecret;
    private readonly string _tokenEndpoint;
    private readonly string _scope;

    private readonly IProducer<string, string> _producer;
    private readonly IConsumer<string, string> _consumer;

    private readonly ConcurrentDictionary<Guid, DateTime> _inflight = new();

    public static DateTime LastSuccessUtc { get; private set; } = DateTime.MinValue;

    public KafkaCanaryService(ILogger<KafkaCanaryService> logger, IConfiguration config)
    {
        _logger = logger;

        _topic = config["KAFKA:TOPIC"] ?? "onprem.test.canary-data.add.v1";

        var bootstrap = config["KAFKA:BOOTSTRAPSERVERS"] ?? "localhost:9092";

        _clientId = config["KAFKA:CLIENTID"] ?? "<client-id>";
        _clientSecret = config["KAFKA:CLIENTSECRET"]!;
        _tokenEndpoint = config["KAFKA:TOKEN_ENDPOINT"] ?? "https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token";
        _scope = config["KAFKA:OAUTH_SCOPE"] ?? "api://<client-id>/.default";
        var sslCaLocation = config["KAFKA:SSLCALOCATION"] ?? "certs/ca.crt";
        

        var baseConfig = new ClientConfig
        {
            AllowAutoCreateTopics = true,
            BootstrapServers = bootstrap,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.OAuthBearer,
            SslCaLocation = sslCaLocation
        };

        _producer = new ProducerBuilder<string, string>(baseConfig)
            .SetOAuthBearerTokenRefreshHandler(TokenRefreshHandler)
            .Build();

        var consumerConfig = new ConsumerConfig(baseConfig)
        {
            GroupId = "kafka-canary-group",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Latest
        };

        _consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetOAuthBearerTokenRefreshHandler(TokenRefreshHandler)
            .Build();

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
            catch (ProduceException<string?, byte> ex)
            {
                PrometheusMetrics.Failures.Inc();
                _logger.LogError("Error producing message 'ProduceException': {Message}", ex.Message);
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

    private async void TokenRefreshHandler(IClient client, string config)
    {
        try
        {
            var (accessToken, expiresIn) = await FetchToken(_tokenEndpoint, _clientId, _clientSecret);
                
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var safeExpiresInMs = Math.Max(1, (expiresIn - 5) * 1000);

            var absoluteExpiry = nowMs + safeExpiresInMs;

            _logger.LogInformation("Setting Kafka OAuth token. lifetimeMs={Lifetime}, rawExpiresIn={Raw}",
                safeExpiresInMs, expiresIn);
            var handler = new JwtSecurityTokenHandler();
            var jwt = handler.ReadJwtToken(accessToken);
            var principalName = jwt.Claims.First(c => c.Type == "sub").Value;

            client.OAuthBearerSetToken(
                accessToken,
                absoluteExpiry,
                principalName,
                null
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "OAuth token refresh failed");
            client.OAuthBearerSetTokenFailure(ex.Message);
        }
    }

    private async Task<(string access_token, long expires_in)> FetchToken(
        string tokenEndpoint, string clientId, string clientSecret)
    {
        using var http = new HttpClient();
        _logger.LogInformation("Fetching access token...");

        var body = new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = clientId,
            ["client_secret"] = clientSecret,
            ["scope"] = _scope
        };

        var response = await http.PostAsync(tokenEndpoint, new FormUrlEncodedContent(body));
        var content = await response.Content.ReadAsStringAsync();

        if (!response.IsSuccessStatusCode)
        {
            _logger.LogError("Failed to fetch token: {Status} {Content}", response.StatusCode, content);
            throw new Exception("Failed to obtain OAuth token");
        }

        var json = JsonSerializer.Deserialize<JsonElement>(content);

        return (
            json.GetProperty("access_token").GetString()!,
            json.GetProperty("expires_in").GetInt64()
        );
    }

    public override void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
        _producer.Dispose();
        base.Dispose();
    }
}
