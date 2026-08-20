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
