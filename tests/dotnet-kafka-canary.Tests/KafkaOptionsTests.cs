using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace dotnet_kafka_canary.Tests;

public sealed class KafkaOptionsTests
{
    private readonly KafkaOptionsValidator _validator = new();

    [Fact]
    public void DefaultsToOAuthBearer()
    {
        var options = new KafkaOptions();

        Assert.Equal(SaslMechanism.OAuthBearer, options.SaslMechanism);
    }

    [Fact]
    public void CreatesOAuthBearerConfigurationWithRefreshHandler()
    {
        var options = CreateValidOAuthOptions();
        var factory = CreateFactory(options);

        var result = factory.Create();

        Assert.Equal(SecurityProtocol.SaslSsl, result.ClientConfig.SecurityProtocol);
        Assert.Equal(SaslMechanism.OAuthBearer, result.ClientConfig.SaslMechanism);
        Assert.NotNull(result.OAuthBearerTokenRefreshHandler);
        Assert.Null(result.ClientConfig.SaslUsername);
        Assert.Null(result.ClientConfig.SaslPassword);
        Assert.Equal(true, result.ProducerConfig.EnableIdempotence);
        Assert.Equal(int.MaxValue, result.ProducerConfig.MessageSendMaxRetries);
        Assert.Equal(options.ProducerMessageTimeoutMs, result.ProducerConfig.MessageTimeoutMs);
        Assert.Equal(options.RetryBackoffMs, result.ProducerConfig.RetryBackoffMs);
        Assert.Equal(options.RetryBackoffMaxMs, result.ProducerConfig.RetryBackoffMaxMs);
    }

    [Fact]
    public void CreatesScramSha512ConfigurationWithoutRefreshHandler()
    {
        var options = new KafkaOptions
        {
            SaslMechanism = SaslMechanism.ScramSha512,
            SaslUsername = "canary-user",
            SaslPassword = "canary-password"
        };
        var factory = CreateFactory(options);

        var result = factory.Create();

        Assert.Equal(SecurityProtocol.SaslSsl, result.ClientConfig.SecurityProtocol);
        Assert.Equal(SaslMechanism.ScramSha512, result.ClientConfig.SaslMechanism);
        Assert.Equal("canary-user", result.ClientConfig.SaslUsername);
        Assert.Equal("canary-password", result.ClientConfig.SaslPassword);
        Assert.Equal("canary-user", result.ProducerConfig.SaslUsername);
        Assert.Equal("canary-password", result.ProducerConfig.SaslPassword);
        Assert.Null(result.OAuthBearerTokenRefreshHandler);
    }

    [Fact]
    public void RejectsMissingOAuthSettings()
    {
        var result = _validator.Validate(null, new KafkaOptions());

        Assert.True(result.Failed);
        Assert.Contains(result.Failures, failure => failure.Contains(nameof(KafkaOptions.ClientId)));
        Assert.Contains(result.Failures, failure => failure.Contains(nameof(KafkaOptions.ClientSecret)));
        Assert.Contains(result.Failures, failure => failure.Contains(nameof(KafkaOptions.TokenEndpoint)));
        Assert.Contains(result.Failures, failure => failure.Contains(nameof(KafkaOptions.OAuthScope)));
    }

    [Fact]
    public void AcceptsWorkloadIdentityWithoutClientSecretSettings()
    {
        var options = new KafkaOptions
        {
            UseWorkloadIdentity = true,
            OAuthScope = "api://kafka/.default"
        };

        Assert.Same(ValidateOptionsResult.Success, _validator.Validate(null, options));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("api://kafka")]
    public void RejectsMissingOrInvalidWorkloadIdentityScope(string? scope)
    {
        var options = new KafkaOptions { UseWorkloadIdentity = true, OAuthScope = scope };

        var result = _validator.Validate(null, options);

        Assert.NotNull(result.Failures);
        Assert.Contains(nameof(KafkaOptions.OAuthScope), Assert.Single(result.Failures));
    }

    [Fact]
    public void RejectsMissingScramSettings()
    {
        var options = new KafkaOptions
        {
            SaslMechanism = SaslMechanism.ScramSha512
        };

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains(result.Failures, failure => failure.Contains(nameof(KafkaOptions.SaslUsername)));
        Assert.Contains(result.Failures, failure => failure.Contains(nameof(KafkaOptions.SaslPassword)));
    }

    [Fact]
    public void RejectsUnsupportedSaslMechanism()
    {
        var options = CreateValidOAuthOptions();
        options.SaslMechanism = SaslMechanism.Plain;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains(
            result.Failures,
            failure => failure.Contains("must be OAuthBearer or ScramSha512"));
    }

    [Fact]
    public void RejectsInvalidProducerRetrySettings()
    {
        var options = CreateValidOAuthOptions();
        options.ProducerMessageTimeoutMs = 0;
        options.RetryBackoffMs = 2_000;
        options.RetryBackoffMaxMs = 1_000;
        options.ConsumeTimeoutMs = 0;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains(result.Failures, failure => failure.Contains(nameof(KafkaOptions.ProducerMessageTimeoutMs)));
        Assert.Contains(result.Failures, failure => failure.Contains(nameof(KafkaOptions.RetryBackoffMaxMs)));
        Assert.Contains(result.Failures, failure => failure.Contains(nameof(KafkaOptions.ConsumeTimeoutMs)));
    }

    [Fact]
    public void AcceptsValidSettingsForBothModes()
    {
        var oauthResult = _validator.Validate(null, CreateValidOAuthOptions());
        var scramResult = _validator.Validate(null, new KafkaOptions
        {
            SaslMechanism = SaslMechanism.ScramSha512,
            SaslUsername = "canary-user",
            SaslPassword = "canary-password"
        });

        Assert.Same(ValidateOptionsResult.Success, oauthResult);
        Assert.Same(ValidateOptionsResult.Success, scramResult);
    }

    private static KafkaOptions CreateValidOAuthOptions()
    {
        return new KafkaOptions
        {
            ClientId = "client-id",
            ClientSecret = "client-secret",
            TokenEndpoint = "https://identity.example.com/oauth2/token",
            OAuthScope = "api://kafka/.default"
        };
    }

    private static KafkaClientConfigurationFactory CreateFactory(KafkaOptions options)
    {
        return new KafkaClientConfigurationFactory(
            Options.Create(options),
            new StubTokenProvider(),
            NullLogger<KafkaClientConfigurationFactory>.Instance);
    }

    private sealed class StubTokenProvider : IKafkaOAuthTokenProvider
    {
        public Task<KafkaOAuthToken> FetchTokenAsync(CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }
    }
}
