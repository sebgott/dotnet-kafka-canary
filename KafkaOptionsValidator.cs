using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace dotnet_kafka_canary;

public sealed class KafkaOptionsValidator : IValidateOptions<KafkaOptions>
{
    public ValidateOptionsResult Validate(string? name, KafkaOptions options)
    {
        var failures = new List<string>();

        Require(options.Topic, nameof(options.Topic), failures);
        Require(options.BootstrapServers, nameof(options.BootstrapServers), failures);
        Require(options.SslCaLocation, nameof(options.SslCaLocation), failures);
        Require(options.GroupId, nameof(options.GroupId), failures);

        switch (options.SaslMechanism)
        {
            case SaslMechanism.OAuthBearer:
                Require(options.ClientId, nameof(options.ClientId), failures);
                Require(options.ClientSecret, nameof(options.ClientSecret), failures);
                Require(options.OAuthScope, nameof(options.OAuthScope), failures);

                if (string.IsNullOrWhiteSpace(options.TokenEndpoint))
                {
                    failures.Add($"{KafkaOptions.SectionName}:{nameof(options.TokenEndpoint)} is required for OAuthBearer.");
                }
                else if (!Uri.TryCreate(options.TokenEndpoint, UriKind.Absolute, out _))
                {
                    failures.Add($"{KafkaOptions.SectionName}:{nameof(options.TokenEndpoint)} must be an absolute URI.");
                }

                break;

            case SaslMechanism.ScramSha512:
                Require(options.SaslUsername, nameof(options.SaslUsername), failures);
                Require(options.SaslPassword, nameof(options.SaslPassword), failures);
                break;

            default:
                failures.Add(
                    $"{KafkaOptions.SectionName}:{nameof(options.SaslMechanism)} must be OAuthBearer or ScramSha512.");
                break;
        }

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }

    private static void Require(string? value, string propertyName, ICollection<string> failures)
    {
        if (string.IsNullOrWhiteSpace(value))
            failures.Add($"{KafkaOptions.SectionName}:{propertyName} is required.");
    }
}
