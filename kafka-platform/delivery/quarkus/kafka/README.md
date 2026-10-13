# Kafka infrastructure (Quarkus stack)

Kafka, Postgres-pgvector, Redis and the network in which the Quarkus stack runs.

## Contents

```
deployment/
├── compose/01-infrastructure.yml    Kafka, Postgres-pgvector, Redis, llm-net
└── scripts/
    ├── sasl-ssl-init.sh             SASL/SSL-Konfiguration der Broker
    └── kafka-pki-setup.sh           Zertifikate erzeugen
config/observability/
├── prometheus.yml
├── otel-collector-config.yaml
└── tempo.yaml
```

## Services

`01-infrastructure.yml` brings up four services in one network (`llm-net`):

| Service | Purpose |
|---|---|
| `kafka` | Broker for the Quarkus stack |
| `postgres-pgvector` | quad database with vector support |
| `redis` | used by quad for sessions and caching |

Deploy the application with the matching overlay from `../quad/deployment/compose/`.

## External Kafka

Instead of the bundled broker, existing infrastructure is used.
The Kafka part of the overlay is then dropped and the connection runs via
`EXTERNAL_KAFKA_BOOTSTRAP_SERVERS` or the corresponding variable in
the quad configuration (`QUAD_MESSAGING_KAFKA_INBOUND_BOOTSTRAP_SERVERS`).

## TLS

The two scripts are the SASL-SSL variant:

```bash
./kafka-pki-setup.sh              # Zertifikate erzeugen
./sasl-ssl-init.sh                # Broker-Konfiguration anwenden
```

They come from `kafka-platform/kafka` and `kafka-platform/scripts` and live
here because the TLS configuration is identical in both delivery branches. A
second place for the same file would be a second place where it can become outdated.
Instructions for certificates, Scram users and the necessary JAAS configurations
are in `kafka-platform/TLS_SASL_README.md`.

## Observability

Prometheus and OTel configuration under `config/observability/`. The
OTel collector exports to Tempo; trace endpoint and sampling are in
`otel-collector-config.yaml`.
