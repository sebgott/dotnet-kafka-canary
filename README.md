# Kafka Canary

This is a simple application that constantly produces and consumes to
test message throughput in Kafka. It logs and produces Prometheus metrics.
Helpful for measuring availability and performance. Developed to be able to
be quickly alerted to issues with Kafka.

Metrics produced:

```txt
kafka_canary_messages_produced_total
kafka_canary_messages_consumed_total
kafka_canary_failures_total
kafka_canary_roundtrip_seconds
```