namespace dotnet_kafka_canary;

public record CanaryMessage(
    string InstanceId,
    Guid MessageId,
    DateTime ProducedAtUtc
);
