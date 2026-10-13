# Quarkus-Laufzeit

quad selbst läuft auf Quarkus. Diese Ablage deckt die Komponenten ab, die neben
der Kafka-Plattform betrieben werden können: die quad-Anwendung und die
Kafka-Infrastruktur, die sie braucht.

Wird nur die Kafka-Plattform ausgeliefert, wird dieser Ordner nicht gebraucht —
siehe `../README.md`.

## `quad/` — quad-Anwendung

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

Die Compose-Overlays sind nummeriert, weil sie zur `deploy/`-Serie des
Quell-Repos gehören und dort in fester Reihenfolge angewendet werden. Ohne die
übrigen Overlays sind sie nicht lauffähig; `01-infrastructure.yml` liegt deshalb
in `../kafka/deployment/compose/`.

### Konfiguration

Die `.env`-Dateien enthalten **Variablenverweise**, keine Werte:

```
POSTGRES_PASSWORD=${PROD_DB_PASSWORD}
OPENAI_API_KEY=${PROD_OPENAI_API_KEY}
KEYCLOAK_ADMIN_PASSWORD=${PROD_KEYCLOAK_ADMIN_PASSWORD}
```

Beim Zielsystem ist für jeden Verweis ein echter Wert zu setzen. Die Dateien
selbst sind bewusst im Repo unversioniert.

## `kafka/` — Kafka-Infrastruktur

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

`01-infrastructure.yml` bringt den Kafka-Cluster, Postgres-pgvector und Redis
gemeinsam mit dem Quad-Netz. Für einen externen Kafka-Cluster wird stattdessen
`EXTERNAL_KAFKA_BOOTSTRAP_SERVERS` gesetzt und der Cluster-Teil entfällt.

Die SASL-SSL-Skripte gehören zu beiden Stacks: `sasl-ssl-init.sh` stammt aus
dem kafka-platform, `kafka-pki-setup.sh` ebenfalls. Sie sind hier abgelegt, weil
die TLS-Variante von Kafka in beiden Delivery-Zweigen identisch ist und doppelt
zu pflegen nur zwei Orte für dieselbe Fehlerquelle wären.
