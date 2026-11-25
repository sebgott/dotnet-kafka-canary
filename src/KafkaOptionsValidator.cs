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
        Require(options.GroupId, nameof(options.GroupId), failures);
        RequirePositive(options.ProducerMessageTimeoutMs, nameof(options.ProducerMessageTimeoutMs), failures);
        RequirePositive(options.RetryBackoffMs, nameof(options.RetryBackoffMs), failures);
        RequirePositive(options.RetryBackoffMaxMs, nameof(options.RetryBackoffMaxMs), failures);

        if (options.RetryBackoffMaxMs < options.RetryBackoffMs)
        {
            failures.Add(
                $"{KafkaOptions.SectionName}:{nameof(options.RetryBackoffMaxMs)} must be greater than or equal to {nameof(options.RetryBackoffMs)}.");
        }

        switch (options.SaslMechanism)
        {
            case SaslMechanism.OAuthBearer:
                Require(options.OAuthScope, nameof(options.OAuthScope), failures);

                if (options.UseWorkloadIdentity)
                {
                    if (!string.IsNullOrWhiteSpace(options.OAuthScope)
                        && !options.OAuthScope.EndsWith("/.default", StringComparison.Ordinal))
                    {
                        failures.Add($"{KafkaOptions.SectionName}:{nameof(options.OAuthScope)} must end with /.default for Azure Workload Identity.");
                    }

                    break;
                }

                Require(options.ClientId, nameof(options.ClientId), failures);
                Require(options.ClientSecret, nameof(options.ClientSecret), failures);

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

    private static void RequirePositive(int value, string propertyName, ICollection<string> failures)
    {
        if (value <= 0)
            failures.Add($"{KafkaOptions.SectionName}:{propertyName} must be greater than zero.");
    }
}
