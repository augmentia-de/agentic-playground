# quad application (Quarkus)

quad itself: quad-core, the Quarkus application, quad-ui, MCP server and the
workflow engine.

## Contents

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

## Compose overlays are part of a series

`05-app-quarkus.yml` and `05-app-minimal.yml` are overlays on a
numbered series; the number specifies the order. They expect the
previous overlays, in particular `01-infrastructure.yml` — which lives in
`../kafka/deployment/compose/`. A single overlay is therefore not
runnable:

```bash
cd delivery/quarkus
docker compose \
  -f kafka/deployment/compose/01-infrastructure.yml \
  -f quad/deployment/compose/05-app-quarkus.yml \
  up -d --build
```

## Configuration

The files under `config/env` contain variable references, not values. For
each one, a real value must be set on the target system:

| Variable | Meaning |
|---|---|
| `POSTGRES_PASSWORD` / `${PROD_DB_PASSWORD}` | Password for the quad database |
| `OPENAI_API_KEY` / `${PROD_OPENAI_API_KEY}` | Model access |
| `KEYCLOAK_ADMIN_PASSWORD` | only with `OIDC_ENABLED=true` |
| `QUARKUS_DATASOURCE_JDBC_URL` | points to the Compose service `postgres-pgvector` by default |
| `QUAD_MESSAGING_KAFKA_INBOUND_BOOTSTRAP_SERVERS` | points to `kafka:9092` |

## Build

`deployment/scripts/build.sh` and `start.sh` are the scripts from
`quad-quarkus/scripts`. They expect the Maven module tree of the source repo
(`quad-core`, `quad-quarkus`, `quad-ui`, …) and do not run from the delivery
alone. Build in the repo instead; copying is only the handover of the artifacts.
