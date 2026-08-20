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

### OAuthBearer

It requires:

```text
KAFKA__CLIENTID
KAFKA__CLIENTSECRET
KAFKA__TOKENENDPOINT
KAFKA__OAUTHSCOPE
```

`deployment.yaml` shows a complete OAuthBearer deployment. Store
`KAFKA__CLIENTSECRET` in a Kubernetes Secret rather than directly in the
manifest.

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