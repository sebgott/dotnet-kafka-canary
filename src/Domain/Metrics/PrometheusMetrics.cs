using Prometheus;

namespace dotnet_kafka_canary.Domain.Metrics;

public static class FailureStage
{
    public const string Produce = "produce";
    public const string Consume = "consume";
    public const string Process = "process";

    public static readonly string[] All = [Produce, Consume, Process];
}

public static class PrometheusMetrics
{
    // 1 ms to ~131 s.
    private static readonly double[] LatencyBuckets = Histogram.ExponentialBuckets(0.001, 2, 18);

    public static readonly Counter MessagesProduced = Prometheus.Metrics
        .CreateCounter("kafka_canary_messages_produced_total", "Total messages acknowledged by the broker");

    public static readonly Counter MessagesConsumed = Prometheus.Metrics
        .CreateCounter("kafka_canary_messages_consumed_total", "Total own messages consumed (completed roundtrips)");

    public static readonly Counter MessagesLost = Prometheus.Metrics
        .CreateCounter("kafka_canary_messages_lost_total",
            "Messages acknowledged by the broker but not consumed within the consume timeout");

    public static readonly Counter Failures = Prometheus.Metrics
        .CreateCounter("kafka_canary_failures_total", "Total failures by stage",
            new CounterConfiguration { LabelNames = ["stage"] });

    public static readonly Gauge InflightMessages = Prometheus.Metrics
        .CreateGauge("kafka_canary_inflight_messages", "Messages produced but not yet consumed");

    public static readonly Gauge LastRoundtripTimestamp = Prometheus.Metrics
        .CreateGauge("kafka_canary_last_roundtrip_timestamp_seconds",
            "Unix timestamp of the last successful roundtrip");

    public static readonly Histogram ProduceLatency = Prometheus.Metrics
        .CreateHistogram("kafka_canary_produce_seconds", "Time from produce to broker acknowledgement in seconds",
            new HistogramConfiguration { Buckets = LatencyBuckets });

    public static readonly Histogram RoundtripLatency = Prometheus.Metrics
        .CreateHistogram("kafka_canary_roundtrip_seconds", "Time from produce to consume in seconds",
            new HistogramConfiguration { Buckets = LatencyBuckets });

    static PrometheusMetrics()
    {
        // Publish every stage at zero so rate() queries return data before the first failure.
        foreach (var stage in FailureStage.All)
            Failures.WithLabels(stage);
    }
}
