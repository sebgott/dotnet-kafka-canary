using Prometheus;

namespace dotnet_kafka_canary.Domain.Metrics;

public static class PrometheusMetrics
{
    public static readonly Counter MessagesProduced = Prometheus.Metrics
        .CreateCounter("kafka_canary_messages_produced_total", "Total produced messages");

    public static readonly Counter MessagesConsumed = Prometheus.Metrics
        .CreateCounter("kafka_canary_messages_consumed_total", "Total consumed messages");

    public static readonly Counter Failures = Prometheus.Metrics
        .CreateCounter("kafka_canary_failures_total", "Total failures");

    public static readonly Histogram RoundtripLatency = Prometheus.Metrics
        .CreateHistogram("kafka_canary_roundtrip_seconds", "Roundtrip time in seconds",
            new HistogramConfiguration
            {
                Buckets = Histogram.ExponentialBuckets(0.01, 2, 15)
            });
}
