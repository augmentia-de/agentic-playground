# Delivery

Delivery folder for both platforms. The layout follows the runtime that is
actually deployed, and the application behind it.

```
delivery/
├── sync-from-source.sh          Mapping + Kopierlogik (Single Source of Truth)
├── quarkus/                     Quarkus-Laufzeit
│   ├── quad/                    quad-Anwendung (quad-core, Quarkus-App, quad-ui)
│   │   ├── deployment/          Dockerfile, Compose-Overlays, Helm-Chart, Skripte
│   │   └── config/              env-Templates, MCP-Konfiguration
│   └── kafka/                   Kafka-Infrastruktur, die der Quarkus-Stack braucht
│       ├── deployment/          Compose-Infrastruktur, PKI-/SASL-Skripte
│       └── config/              Prometheus, OTel-Collector, Tempo
└── spring/                      Spring-Boot-Laufzeit
    ├── quad/                    quad-workflow-worker (quad-core im Worker)
    │   ├── deployment/          Dockerfiles, Build-Hilfe
    │   └── config/              erforderliche Worker-Einstellungen
    └── kafka/                   Kafka-Plattform (Orchestrator + Agent-Worker)
        ├── deployment/          Dockerfiles, Compose, Helm-Chart, Skripte
        └── config/              Helm-Overlay, Prometheus, OTel, Grafana
```

## Mapping

| Folder | Deploys | Source |
|---|---|---|
| `quarkus/quad` | quad-backend, quad-ui, MCP server | `quad-quarkus/docker`, `quad-quarkus/deploy`, `quad-quarkus/scripts` |
| `quarkus/kafka` | Kafka, Postgres-pgvector, Redis for the Quarkus stack | `quad-quarkus/deploy/01-infrastructure.yml`, `kafka-platform/kafka`, `kafka-platform/scripts` |
| `spring/quad` | quad-workflow-worker | `kafka-platform/docker/Dockerfile-quad-workflow-worker*` |
| `spring/kafka` | agent-orchestrator, agent-worker, complete stack | `kafka-platform/docker`, `docker-compose*.yml`, `helm`, `grafana`, `prometheus` |

## Two paths, one goal

A workflow designed in quad is executed on the Kafka platform. This requires
both runtimes, but not necessarily both installed:

**Kafka platform only** — the standalone variant. `spring/kafka` plus
`spring/quad` for the worker. The workflow is taken over via
`POST /api/v1/unified/workflows/from-quad`, the agents via
`POST /api/v1/unified/agents/convert`. The runnable example is in
`kafka-platform/examples/quad-to-kafka/` with a README.

**Quad plus platform** — when quad itself should also run (own workflows,
quad-ui, MCP). Then additionally `quarkus/quad` and `quarkus/kafka`.

## Generating and renewing

The folders are not maintained by hand, but generated:

```bash
cd delivery && ./sync-from-source.sh              # erneuert die Kopien
cd delivery && ./sync-from-source.sh --clean      # leert vorher deployment/ und config/
```

The mapping is fully contained in the script. After a change to a Dockerfile,
compose file or chart, one run is enough — folders and script cannot drift apart,
because there is only one source for the mapping.

**Which folders are generated and which are not:**

| Path | Origin |
|---|---|
| `*/deployment/`, `*/config/` | generated — entirely from `sync-from-source.sh`, do not change by hand |
| `README.md` (all levels) | maintained by hand, not part of the mapping |

Therefore **no `rm -rf quarkus spring`**. The script writes additively and leaves
the README files in place; `--clean` specifically removes only the generated
folders. `--clean` also fixes the drift problem of an additive run: a file
deleted in the source repo would otherwise remain as an outdated copy.

The script copies **exclusively git-tracked files**. In both repos, `.env`,
`.env.prod`, `.env.secrets.cloud` and `docker/secrets/` are ignored and
are therefore skipped — deliberately: a delivery is handed over, and
a key once delivered can no longer be removed from the working tree. For
configuration, the `config` folders therefore provide templates with
variable references (`${PROD_DB_PASSWORD}`), which must be set on the target system.

To be set before the first deployment:

| Variable | Where | Why |
|---|---|---|
| `WORKER_QUAD_AGENT_SOURCE=db` | `spring/quad` | Without this the worker only knows the static YAML list and reports `Unknown agent type` for every agent imported via `/agents/convert`. |
| `OPENAI_API_KEY` | `spring/quad` | Without a key the worker falls back to a mock model. |
| the agents' `modelId` | `spring/quad` | Must be `simple` — the worker registers exactly this one key. |
| `EXTERNAL_KAFKA_BOOTSTRAP_SERVERS`, `POSTGRES_URL` | `spring/kafka` | Mandatory for `docker-compose.minimal.yml`, checked at startup. |
| `postgresqlPassword` | `spring/kafka/config/helm` | Chart default is a fixed value and should be replaced. |

## Known limits

- **Shared files.** `docker-compose.minimal.yml` and `start-minimal.sh`
  start the orchestrator *and* the worker and therefore live entirely under
  `spring/kafka`. For the worker alone, the Dockerfiles are stored in
  `spring/quad/deployment/docker`.
- **References to source repos.** Compose overlays and scripts expect paths
  within the respective repo (`../quad-core`, Maven modules). They are copied as
  a reference, not as a standalone build artifact — the build runs in the
  respective repo.
- **Helm charts are copied, not adapted.** `deployment/helm` is the
  chart state from the source repo. Adjustments for the target system belong in an
  overlay under `config/helm`, not in the chart.
- **Security not included.** Authentication is off by default in both platforms
  (`app.security.enabled=false`). Before a delivery with network access, this must
  be decided — see the repo root of `kafka-platform`.
