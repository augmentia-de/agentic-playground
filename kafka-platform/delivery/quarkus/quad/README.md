# quad-Anwendung (Quarkus)

quad selbst: quad-core, die Quarkus-Anwendung, quad-ui, MCP-Server und der
Workflow-Engine.

## Inhalt

```
deployment/
├── docker/backend.Dockerfile
├── compose/
│   ├── 05-app-quarkus.yml     Quarkus mit UI, MCP und OIDC
│   └── 05-app-minimal.yml     minimal, ohne UI und MCP
├── helm/                      Chart: backend, frontend, HPA, Ingress
├── nginx-minimal.conf
└── scripts/{build.sh,start.sh}
config/
├── env/
│   ├── .env.example
│   ├── .env.prod.example
│   └── .env.secrets.cloud.example
└── mcp-config.json
```

## Compose-Overlays sind Teil einer Serie

`05-app-quarkus.yml` und `05-app-minimal.yml` sind Overlays auf eine
nummerierte Serie; die Nummer gibt die Reihenfolge an. Sie erwarten die
vorherigen Overlays, insbesondere `01-infrastructure.yml` — das liegt in
`../kafka/deployment/compose/`. Ein einzelnes Overlay ist deshalb nicht
lauffähig:

```bash
cd delivery/quarkus
docker compose \
  -f kafka/deployment/compose/01-infrastructure.yml \
  -f quad/deployment/compose/05-app-quarkus.yml \
  up -d --build
```

## Konfiguration

Die Dateien unter `config/env` enthalten Variablenverweise, keine Werte. Für
jeden ist am Zielsystem ein echter Wert zu setzen:

| Variable | Bedeutung |
|---|---|
| `POSTGRES_PASSWORD` / `${PROD_DB_PASSWORD}` | Passwort für die quad-Datenbank |
| `OPENAI_API_KEY` / `${PROD_OPENAI_API_KEY}` | Modellzugang |
| `KEYCLOAK_ADMIN_PASSWORD` | nur bei `OIDC_ENABLED=true` |
| `QUARKUS_DATASOURCE_JDBC_URL` | zeigt im Default auf den Compose-Service `postgres-pgvector` |
| `QUAD_MESSAGING_KAFKA_INBOUND_BOOTSTRAP_SERVERS` | zeigt auf `kafka:9092` |

## Build

`deployment/scripts/build.sh` und `start.sh` sind die Skripte aus
`quad-quarkus/scripts`. Sie erwarten den Maven-Modulbaum des Quell-Repos
(`quad-core`, `quad-quarkus`, `quad-ui`, …) und laufen nicht aus der Delivery
allein. Bauen daher im Repo, kopieren ist nur die Übergabe der Artefakte.
