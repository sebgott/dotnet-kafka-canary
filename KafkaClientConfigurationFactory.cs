using System.IdentityModel.Tokens.Jwt;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace dotnet_kafka_canary;

public sealed record KafkaClientConfiguration(
    ClientConfig ClientConfig,
    ProducerConfig ProducerConfig,
    Action<IClient, string>? OAuthBearerTokenRefreshHandler);

public sealed class KafkaClientConfigurationFactory
{
    private readonly KafkaOptions _options;
    private readonly IKafkaOAuthTokenProvider _tokenProvider;
    private readonly ILogger<KafkaClientConfigurationFactory> _logger;

    public KafkaClientConfigurationFactory(
        IOptions<KafkaOptions> options,
        IKafkaOAuthTokenProvider tokenProvider,
        ILogger<KafkaClientConfigurationFactory> logger)
    {
        _options = options.Value;
        _tokenProvider = tokenProvider;
        _logger = logger;
    }

    public KafkaClientConfiguration Create()
    {
        var clientConfig = new ClientConfig
        {
            AllowAutoCreateTopics = true,
            BootstrapServers = _options.BootstrapServers,
            RetryBackoffMs = _options.RetryBackoffMs,
            RetryBackoffMaxMs = _options.RetryBackoffMaxMs,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = _options.SaslMechanism,
            SslCaLocation = _options.SslCaLocation
        };

        Action<IClient, string>? oAuthBearerTokenRefreshHandler;
        if (_options.SaslMechanism == SaslMechanism.ScramSha512)
        {
            clientConfig.SaslUsername = _options.SaslUsername;
            clientConfig.SaslPassword = _options.SaslPassword;
            oAuthBearerTokenRefreshHandler = null;
        }
        else
        {
            oAuthBearerTokenRefreshHandler = RefreshOAuthBearerToken;
        }

        var producerConfig = new ProducerConfig(clientConfig)
        {
            EnableIdempotence = true,
            MessageSendMaxRetries = int.MaxValue,
            MessageTimeoutMs = _options.ProducerMessageTimeoutMs
        };

        return new KafkaClientConfiguration(
            clientConfig,
            producerConfig,
            oAuthBearerTokenRefreshHandler);
    }

    private async void RefreshOAuthBearerToken(IClient client, string config)
    {
        try
        {
            var token = await _tokenProvider.FetchTokenAsync();
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var lifetimeMs = Math.Max(1, token.ExpiresInSeconds - 5) * 1000;
            var absoluteExpiry = nowMs + lifetimeMs;
            var jwt = new JwtSecurityTokenHandler().ReadJwtToken(token.AccessToken);
            var principalName = jwt.Claims.FirstOrDefault(claim => claim.Type == "sub")?.Value;

            if (string.IsNullOrWhiteSpace(principalName))
                throw new InvalidOperationException("Kafka OAuth token does not contain a subject claim.");

            _logger.LogInformation(
                "Setting Kafka OAuth token with lifetime {LifetimeMs} ms",
                lifetimeMs);
            client.OAuthBearerSetToken(
                token.AccessToken,
                absoluteExpiry,
                principalName,
                null);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Kafka OAuth token refresh failed");
            client.OAuthBearerSetTokenFailure(ex.Message);
        }
    }
}
