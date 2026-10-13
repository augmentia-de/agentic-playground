# Quarkus runtime

quad itself runs on Quarkus. This folder covers the components that can be
operated alongside the Kafka platform: the quad application and the
Kafka infrastructure it needs.

If only the Kafka platform is delivered, this folder is not needed —
see `../README.md`.

## `quad/` — quad application

```
deployment/
├── docker/backend.Dockerfile
├── compose/
│   ├── 05-app-quarkus.yml        Overlay für Quarkus (Variante mit UI, MCP, OIDC)
│   └── 05-app-minimal.yml        Overlay minimal, ohne UI und MCP
├── helm/                         Chart: quad-backend, quad-ui, HPA, Ingress
├── nginx-minimal.conf
└── scripts/{build.sh,start.sh}
config/
├── env/
│   ├── .env.example                  Grundgerüst aller Variablen
│   ├── .env.prod.example             Produktionsprofil
│   └── .env.secrets.cloud.example    Cloud-Variante, nur Variablenverweise
└── mcp-config.json
```

The Compose overlays are numbered because they belong to the `deploy/` series of
the source repo and are applied there in a fixed order. Without the
other overlays they are not runnable; `01-infrastructure.yml` therefore lives
in `../kafka/deployment/compose/`.

### Configuration

The `.env` files contain **variable references**, not values:

```
POSTGRES_PASSWORD=${PROD_DB_PASSWORD}
OPENAI_API_KEY=${PROD_OPENAI_API_KEY}
KEYCLOAK_ADMIN_PASSWORD=${PROD_KEYCLOAK_ADMIN_PASSWORD}
```

On the target system, a real value must be set for each reference. The files
themselves are deliberately unversioned in the repo.

## `kafka/` — Kafka infrastructure

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

`01-infrastructure.yml` brings up the Kafka cluster, Postgres-pgvector and Redis
together with the quad network. For an external Kafka cluster,
`EXTERNAL_KAFKA_BOOTSTRAP_SERVERS` is set instead and the cluster part is dropped.

The SASL-SSL scripts belong to both stacks: `sasl-ssl-init.sh` comes from
the kafka-platform, `kafka-pki-setup.sh` as well. They are stored here because
the TLS variant of Kafka is identical in both delivery branches and maintaining
it twice would only mean two places for the same source of error.
