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

## Project layout

```txt
src/      application (dotnet-kafka-canary.csproj)
tests/    unit tests
```

Build and test everything with `dotnet test dotnet-kafka-canary.slnx`. The
container image is built from the repository root, e.g. `podman build -t kafka-canary .`

## CloudEvents

Canary messages are produced and consumed as
[CloudEvents](https://cloudevents.io/) in Kafka **binary content mode**. The
CloudEvent attributes are carried as Kafka headers and the message value only
contains the JSON payload:

| Header | Value |
| --- | --- |
| `ce_specversion` | `1.0` |
| `ce_id` | Canary message ID |
| `ce_source` | `urn:dotnet-kafka-canary:<instance id>` |
| `ce_type` | `dotnet.kafka.canary.message.v1` |
| `ce_time` | Produce timestamp |
| `ce_traceparent` / `ce_tracestate` | CloudEvents distributed tracing extension |
| `content-type` | `application/json` |

The Kafka message key is the CloudEvent `partitionkey` (the canary instance
ID). Messages on the topic that are not canary CloudEvents are logged, skipped
and committed.

## Tracing

The canary uses OpenTelemetry tracing. Each produce creates a `send <topic>`
producer span and each consumed message creates a `process <topic>` consumer
span. The trace context is propagated in the W3C `traceparent`/`tracestate`
(and `baggage`) Kafka headers, as well as the CloudEvents `ce_traceparent`
extension, which is used as a fallback when extracting the context. Incoming
HTTP requests (excluding `/metrics` and `/health`) and outgoing HTTP calls,
such as OAuth token requests, are also traced.

Traces are exported over OTLP only when an endpoint is configured with the
standard OpenTelemetry environment variables, for example:

| Environment variable | Example |
| --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector:4317` |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` (default) or `http/protobuf` |
| `OTEL_SERVICE_NAME` | `dotnet-kafka-canary` (default) |

## Grafana dashboard

Import [`dashboards/kafka-canary.json`](dashboards/kafka-canary.json) into
Grafana and select the Prometheus datasource. The dashboard includes
namespace and pod filters, producer and consumer throughput, delivery failures,
the producer/consumer delivery gap, and p50/p95/p99 round-trip latency.

## Kafka authentication

The canary connects with `SASL_SSL` and supports `OAuthBearer` and
`ScramSha512`. Configuration uses the `KAFKA` section; in environment
variables, nested configuration is represented by a double underscore.

Shared settings:

| Environment variable | Default |
| --- | --- |
| `KAFKA__SASLMECHANISM` | `OAuthBearer` |
| `KAFKA__BOOTSTRAPSERVERS` | `localhost:9092` |
| `KAFKA__TOPIC` | `onprem.test.canary-data.add.v1` |
| `KAFKA__GROUPID` | `kafka-canary-group` |
| `KAFKA__SSLCALOCATION` | `certs/ca.crt` |
| `KAFKA__PRODUCERMESSAGETIMEOUTMS` | `900000` (15 minutes) |
| `KAFKA__RETRYBACKOFFMS` | `1000` |
| `KAFKA__RETRYBACKOFFMAXMS` | `10000` |

The producer is idempotent and retries retriable delivery failures until the
configured message timeout expires. The default 15-minute delivery window
keeps a broker rolling update from being reported as a failed canary send while
preventing indefinite pending messages.

### OAuthBearer

By default, OAuthBearer uses client-secret authentication and requires:

```text
KAFKA__CLIENTID
KAFKA__CLIENTSECRET
KAFKA__TOKENENDPOINT
KAFKA__OAUTHSCOPE
```

`deployment.yaml` shows a complete OAuthBearer deployment. Store
`KAFKA__CLIENTSECRET` in a Kubernetes Secret rather than directly in the
manifest.

#### Microsoft Entra Workload Identity

[`deployment-workload-identity.yaml`](deployment-workload-identity.yaml)
includes a dedicated Kubernetes service account, a deployment with
`KAFKA__USEWORKLOADIDENTITY=true`, and a Service. This mode uses Azure's
`WorkloadIdentityCredential` directly and does not fall back to a client secret
or another Azure credential.

1. Enable the cluster's OIDC issuer and
   [Microsoft Entra Workload ID](https://learn.microsoft.com/en-us/azure/aks/workload-identity-deploy-cluster).
   Create a federated identity credential on the Entra app registration or
   user-assigned managed identity. Use the cluster's OIDC issuer,
   subject `system:serviceaccount:<namespace>:kafka-canary-workload-identity`, and audience
   `api://AzureADTokenExchange`. Ensure Kafka authorizes the resulting identity
   to produce and consume on the canary topic and use its consumer group.
2. In the example manifest, replace the service account's Entra client ID,
   set `KAFKA__OAUTHSCOPE` to the Kafka API scope, and point the image at a
   build that supports Workload Identity. Adjust the broker and topic as
   needed and make sure the `kafka-cluster-ca-cert` Secret in the target
   namespace contains `ca.crt`.
3. Apply the manifest to the same namespace used in the federated subject:

   ```sh
   kubectl apply --namespace YOUR_NAMESPACE -f deployment-workload-identity.yaml
   ```

The workload identity webhook injects `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`,
and `AZURE_FEDERATED_TOKEN_FILE` into newly created pods. If the identity is
in a different tenant than the cluster default, add the
`azure.workload.identity/tenant-id` annotation to the service account.

### SCRAM-SHA-512

Select SCRAM-SHA-512 and provide its credentials:

```yaml
- name: KAFKA__SASLMECHANISM
  value: "ScramSha512"
- name: KAFKA__SASLUSERNAME
  valueFrom:
    secretKeyRef:
      name: kafka-canary-scram
      key: username
- name: KAFKA__SASLPASSWORD
  valueFrom:
    secretKeyRef:
      name: kafka-canary-scram
      key: password
```

Remove the OAuth-specific variables when using SCRAM. The application
validates shared and mode-specific settings during startup and reports
missing or unsupported configuration before starting the canary.