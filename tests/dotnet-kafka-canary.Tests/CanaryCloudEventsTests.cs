using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using dotnet_kafka_canary.Domain.CloudEvents;
using dotnet_kafka_canary.Domain.Tracing;
using Xunit;

namespace dotnet_kafka_canary.Tests;

public sealed class CanaryCloudEventsTests
{
    private static readonly CanaryMessage Message = new(
        "instance-1",
        Guid.Parse("7d1c4a8e-0f53-4a8e-9c7b-2d8f1e5a6b3c"),
        new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc));

    [Fact]
    public void WritesCloudEventAttributesAsHeadersAndDataAsValue()
    {
        var kafkaMessage = CanaryCloudEvents.ToKafkaMessage(Message);

        Assert.Equal("instance-1", kafkaMessage.Key);
        Assert.Equal("1.0", Header(kafkaMessage.Headers, "ce_specversion"));
        Assert.Equal(Message.MessageId.ToString(), Header(kafkaMessage.Headers, "ce_id"));
        Assert.Equal(CanaryCloudEvents.EventType, Header(kafkaMessage.Headers, "ce_type"));
        Assert.Equal("urn:dotnet-kafka-canary:instance-1", Header(kafkaMessage.Headers, "ce_source"));
        Assert.StartsWith(CanaryCloudEvents.DataContentType, Header(kafkaMessage.Headers, "content-type"));

        var value = Encoding.UTF8.GetString(kafkaMessage.Value);
        Assert.DoesNotContain("specversion", value);
        Assert.Contains(Message.MessageId.ToString(), value);
    }

    [Fact]
    public void RoundTripsCanaryMessage()
    {
        var kafkaMessage = CanaryCloudEvents.ToKafkaMessage(Message);

        Assert.True(CanaryCloudEvents.TryRead(kafkaMessage, out var cloudEvent, out var data));
        Assert.Equal(Message, data);
        Assert.Equal(Message.MessageId.ToString(), cloudEvent.Id);
    }

    [Fact]
    public void RejectsNonCloudEventMessages()
    {
        var kafkaMessage = new Message<string?, byte[]>
        {
            Key = "key",
            Value = Encoding.UTF8.GetBytes("{}"),
            Headers = []
        };

        Assert.False(CanaryCloudEvents.TryRead(kafkaMessage, out _, out _));
    }

    [Fact]
    public void PropagatesTraceContextThroughHeaders()
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == KafkaCanaryTelemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);

        using var activity = KafkaCanaryTelemetry.StartProduceActivity("topic", Message.InstanceId, Message.MessageId);
        Assert.NotNull(activity);

        var kafkaMessage = CanaryCloudEvents.ToKafkaMessage(Message, activity);
        KafkaCanaryTelemetry.Inject(activity, kafkaMessage.Headers);

        Assert.Equal(activity.Id, Header(kafkaMessage.Headers, "traceparent"));
        Assert.Equal(activity.Id, Header(kafkaMessage.Headers, "ce_traceparent"));

        var extracted = KafkaCanaryTelemetry.Extract(kafkaMessage.Headers);
        Assert.Equal(activity.TraceId, extracted.ActivityContext.TraceId);
        Assert.Equal(activity.SpanId, extracted.ActivityContext.SpanId);
    }

    [Fact]
    public void FallsBackToCloudEventTraceParentHeader()
    {
        const string traceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        var headers = new Headers { { "ce_traceparent", Encoding.UTF8.GetBytes(traceParent) } };

        var extracted = KafkaCanaryTelemetry.Extract(headers);

        Assert.Equal("0af7651916cd43dd8448eb211c80319c", extracted.ActivityContext.TraceId.ToHexString());
        Assert.Equal("b7ad6b7169203331", extracted.ActivityContext.SpanId.ToHexString());
    }

    private static string? Header(Headers headers, string key) =>
        headers.TryGetLastBytes(key, out var value) ? Encoding.UTF8.GetString(value) : null;
}
