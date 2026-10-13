# Kafka-Infrastruktur (Quarkus-Stack)

Kafka, Postgres-pgvector, Redis und das Netz, in dem der Quarkus-Stack läuft.

## Inhalt

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

`01-infrastructure.yml` bringt vier Dienste in einem Netz (`llm-net`):

| Service | Zweck |
|---|---|
| `kafka` | Broker für den Quarkus-Stack |
| `postgres-pgvector` | quad-Datenbank mit Vektorunterstützung |
| `redis` | von quad für Sessions und Caching genutzt |

Anwendung auflegen mit dem passenden Overlay aus `../quad/deployment/compose/`.

## Externes Kafka

Statt des mitgelieferten Brokers wird eine bestehende Infrastruktur verwendet.
Dann entfällt der Kafka-Teil des Overlays und die Verbindung läuft über
`EXTERNAL_KAFKA_BOOTSTRAP_SERVERS` beziehungsweise die entsprechende Variable in
der quad-Konfiguration (`QUAD_MESSAGING_KAFKA_INBOUND_BOOTSTRAP_SERVERS`).

## TLS

Die beiden Skripte sind die SASL-SSL-Variante:

```bash
./kafka-pki-setup.sh              # Zertifikate erzeugen
./sasl-ssl-init.sh                # Broker-Konfiguration anwenden
```

Sie stammen aus `kafka-platform/kafka` und `kafka-platform/scripts` und liegen
hier, weil die TLS-Konfiguration in beiden Delivery-Zweigen identisch ist. Ein
zweiter Ort für dieselbe Datei wäre eine zweite Stelle, an der sie veralten kann.
Anleitung für Zertifikate, Scram-Nutzer und die nötigen Jaas-Konfigurationen
steht in `kafka-platform/TLS_SASL_README.md`.

## Observability

Prometheus- und OTel-Konfiguration unter `config/observability/`. Der
OTel-Collector exportiert nach Tempo; Trace-Endpoint und Sampling stehen in
`otel-collector-config.yaml`.
