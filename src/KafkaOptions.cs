using Confluent.Kafka;

namespace dotnet_kafka_canary;

public sealed class KafkaOptions
{
    public const string SectionName = "KAFKA";

    public string Topic { get; set; } = "onprem.test.canary-data.add.v1";
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string SslCaLocation { get; set; } = "certs/ca.crt";
    public string GroupId { get; set; } = "kafka-canary-group";
    public SaslMechanism SaslMechanism { get; set; } = SaslMechanism.OAuthBearer;
    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }
    public bool UseWorkloadIdentity { get; set; }
    public string? ClientId { get; set; }
    public string? ClientSecret { get; set; }
    public string? TokenEndpoint { get; set; }
    public string? OAuthScope { get; set; }
    public int ProducerMessageTimeoutMs { get; set; } = 900_000;
    public int RetryBackoffMs { get; set; } = 1_000;
    public int RetryBackoffMaxMs { get; set; } = 10_000;
}
