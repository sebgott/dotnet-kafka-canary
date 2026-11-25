using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace dotnet_kafka_canary.Domain.Tracing;

public static class KafkaCanaryTelemetry
{
    public const string ActivitySourceName = "dotnet-kafka-canary";

    private const string CloudEventTraceParentHeader = "ce_traceparent";
    private const string CloudEventTraceStateHeader = "ce_tracestate";

    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    private static readonly TextMapPropagator Propagator = new CompositeTextMapPropagator(
    [
        new TraceContextPropagator(),
        new BaggagePropagator()
    ]);

    public static Activity? StartProduceActivity(string topic, string key, Guid messageId)
    {
        var activity = ActivitySource.StartActivity($"send {topic}", ActivityKind.Producer);
        if (activity is null)
            return null;

        activity.SetTag("messaging.system", "kafka");
        activity.SetTag("messaging.operation.type", "send");
        activity.SetTag("messaging.operation.name", "send");
        activity.SetTag("messaging.destination.name", topic);
        activity.SetTag("messaging.kafka.message.key", key);
        activity.SetTag("messaging.message.id", messageId.ToString());
        activity.SetTag("cloudevents.event_id", messageId.ToString());
        activity.SetTag("cloudevents.event_type", CloudEvents.CanaryCloudEvents.EventType);
        return activity;
    }

    public static Activity? StartProcessActivity(
        ConsumeResult<string?, byte[]> result,
        string consumerGroup,
        PropagationContext parentContext)
    {
        var activity = ActivitySource.StartActivity(
            $"process {result.Topic}",
            ActivityKind.Consumer,
            parentContext.ActivityContext);
        if (activity is null)
            return null;

        activity.SetTag("messaging.system", "kafka");
        activity.SetTag("messaging.operation.type", "process");
        activity.SetTag("messaging.operation.name", "process");
        activity.SetTag("messaging.destination.name", result.Topic);
        activity.SetTag("messaging.destination.partition.id", result.Partition.Value.ToString());
        activity.SetTag("messaging.consumer.group.name", consumerGroup);
        activity.SetTag("messaging.kafka.offset", result.Offset.Value);
        if (result.Message.Key is not null)
            activity.SetTag("messaging.kafka.message.key", result.Message.Key);
        return activity;
    }

    /// <summary>Writes W3C trace context (traceparent/tracestate) and baggage headers.</summary>
    public static void Inject(Activity? activity, Headers headers)
    {
        if (activity is null)
            return;

        Propagator.Inject(
            new PropagationContext(activity.Context, Baggage.Current),
            headers,
            static (carrier, key, value) =>
            {
                carrier.Remove(key);
                carrier.Add(key, Encoding.UTF8.GetBytes(value));
            });
    }

    /// <summary>
    /// Reads W3C trace context headers, falling back to the CloudEvents distributed tracing
    /// extension headers (ce_traceparent/ce_tracestate) when they are absent.
    /// </summary>
    public static PropagationContext Extract(Headers? headers)
    {
        if (headers is null)
            return default;

        var context = Propagator.Extract(default, headers, static (carrier, key) =>
            carrier.TryGetLastBytes(key, out var value)
                ? [Encoding.UTF8.GetString(value)]
                : []);

        if (context.ActivityContext.IsValid())
            return context;

        if (headers.TryGetLastBytes(CloudEventTraceParentHeader, out var traceParent))
        {
            headers.TryGetLastBytes(CloudEventTraceStateHeader, out var traceState);
            if (ActivityContext.TryParse(
                    Encoding.UTF8.GetString(traceParent),
                    traceState is null ? null : Encoding.UTF8.GetString(traceState),
                    isRemote: true,
                    out var activityContext))
            {
                return new PropagationContext(activityContext, context.Baggage);
            }
        }

        return context;
    }
}
