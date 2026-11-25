using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Extensions;
using CloudNative.CloudEvents.Kafka;
using CloudNative.CloudEvents.SystemTextJson;
using Confluent.Kafka;

namespace dotnet_kafka_canary.Domain.CloudEvents;

/// <summary>
/// Maps canary messages to and from binary-mode CloudEvents on Kafka: CloudEvent attributes are
/// carried as <c>ce_*</c> headers and the message value contains only the JSON data.
/// </summary>
public static class CanaryCloudEvents
{
    public const string EventType = "dotnet.kafka.canary.message.v1";
    public const string DataContentType = "application/json";

    // CloudEvents Distributed Tracing extension (ce_traceparent / ce_tracestate headers in binary mode).
    public static readonly CloudEventAttribute TraceParentAttribute =
        CloudEventAttribute.CreateExtension("traceparent", CloudEventAttributeType.String);

    public static readonly CloudEventAttribute TraceStateAttribute =
        CloudEventAttribute.CreateExtension("tracestate", CloudEventAttributeType.String);

    public static readonly IReadOnlyList<CloudEventAttribute> ExtensionAttributes =
    [
        Partitioning.PartitionKeyAttribute,
        TraceParentAttribute,
        TraceStateAttribute
    ];

    private static readonly CloudEventFormatter Formatter = new JsonEventFormatter<CanaryMessage>();

    public static Uri CreateSource(string instanceId) => new($"urn:dotnet-kafka-canary:{instanceId}");

    public static Message<string?, byte[]> ToKafkaMessage(CanaryMessage message, Activity? activity = null)
    {
        var cloudEvent = new CloudEvent(CloudEventsSpecVersion.V1_0, ExtensionAttributes)
        {
            Id = message.MessageId.ToString(),
            Source = CreateSource(message.InstanceId),
            Type = EventType,
            Time = new DateTimeOffset(DateTime.SpecifyKind(message.ProducedAtUtc, DateTimeKind.Utc)),
            DataContentType = DataContentType,
            Data = message
        };
        cloudEvent.SetPartitionKey(message.InstanceId);

        if (activity is not null)
        {
            cloudEvent[TraceParentAttribute] = activity.Id;
            if (!string.IsNullOrEmpty(activity.TraceStateString))
                cloudEvent[TraceStateAttribute] = activity.TraceStateString;
        }

        return cloudEvent.ToKafkaMessage(ContentMode.Binary, Formatter);
    }

    public static bool TryRead(
        Message<string?, byte[]> message,
        [NotNullWhen(true)] out CloudEvent? cloudEvent,
        [NotNullWhen(true)] out CanaryMessage? data)
    {
        cloudEvent = null;
        data = null;

        if (!message.IsCloudEvent())
            return false;

        var parsed = message.ToCloudEvent(Formatter, ExtensionAttributes);
        if (parsed.Data is not CanaryMessage canaryMessage)
            return false;

        cloudEvent = parsed;
        data = canaryMessage;
        return true;
    }
}
