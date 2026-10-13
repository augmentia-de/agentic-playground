# Kafka platform

Orchestrator and agent worker on Spring Boot. The orchestrator accepts
workflow starts and results and drives the DAG; the workers execute
individual tasks.

## Contents

```
deployment/
├── docker/
│   ├── Dockerfile-agent-orchestrator
│   └── Dockerfile-agent-worker
├── compose/
│   ├── docker-compose.yml              vollständiger Stack inkl. Infrastruktur
│   ├── docker-compose.minimal.yml      nur Orchestrator + Worker, externe Infrastruktur
│   └── compose-extensions.yml
├── helm/                               Chart: Orchestrator, Worker, Kafka
└── scripts/
    ├── e2e-test.sh
    └── vault-approle-init.sh
config/
├── helm/values.delivery.yaml           Overlay: die Werte, die zu setzen sind
└── observability/
    ├── prometheus.yml
    ├── otel-config.yaml
    └── grafana/                        Dashboards, Datasource, Provisioning
```

## Two startup paths

**Complete stack.** `docker-compose.yml` brings along Kafka, Postgres, Redis,
Vault, Prometheus, Grafana, Jaeger and the OTel collector.

```bash
cd deployment/compose
docker compose up -d --build
```

**Against existing infrastructure.** `docker-compose.minimal.yml` starts only
the orchestrator and worker and connects Kafka, Postgres and Redis from outside.
Both variables are mandatory and are checked at startup:

```bash
export EXTERNAL_KAFKA_BOOTSTRAP_SERVERS=kafka.example.internal:9092
export POSTGRES_URL=jdbc:postgresql://postgres.example.internal:5432/agent_platform
export POSTGRES_USER=agent
export POSTGRES_PASSWORD=...

cd deployment/compose
docker compose -f docker-compose.minimal.yml up -d --build
```

For the complete lifecycle, `scripts/start-minimal.sh` is included
(`build`, `start`, `stop`, `status`, `logs`).

## Helm

```bash
helm install agent-platform deployment/helm \
  -f config/helm/values.delivery.yaml \
  -f meine-umgebung.yaml
```

`deployment/helm` is the chart state from the source repo and is not
changed. Target system adjustments belong in an overlay under `config/helm`.

The chart default starts Kafka with one replica and Postgres with the
password `agent`. Both should be replaced in a delivery, see
`config/helm/values.delivery.yaml`.

## Two workers

`compose/docker-compose.two-workers.yml` starts the orchestrator plus **two**
`quad-workflow-worker`. Both share one consumer group
(`WORKER_QUAD_AGENT_SOURCE=db`, `WORKER_QUAD_GROUP_ID=quad-workflow-worker`), so that
Kafka distributes the partitions.

```bash
export EXTERNAL_KAFKA_BOOTSTRAP_SERVERS=kafka.example:9092
export POSTGRES_URL=jdbc:postgresql://postgres.example:5432/agent_platform
export POSTGRES_USER=agent POSTGRES_PASSWORD=...
export OPENAI_API_KEY=... OPENAI_MODEL=...

cd deployment/compose
docker compose -f docker-compose.two-workers.yml up -d --build
```

Host ports can be set via `ORCH_HOST_PORT`, `WORKER1_HOST_PORT`, `WORKER2_HOST_PORT`
(default 8080/8092/8093). A fixed port mapping otherwise collides with
something on the host, and the error only appears at `up` as an
opaque `address already in use` from the daemon.

### Measured distribution

Three containers, Kafka single-node, 120 partitions (10 agent types × 12):

| Check | Result |
|---|---|
| Partitions per instance | **60 / 60** |
| of which per agent topic (`ex-market-scout`, `-analyst`, `-writer`) | **6 / 6** |
| 7 workflows, 18 executed tasks | **8 on worker-1, 12 on worker-2** |

### What two workers bring — and what they do not

Throughput grows with the **number of concurrently running workflows**, not
within a workflow. The reason is the task key: it is the `workflow_id`, and
all agent-type topics have 12 partitions. Therefore all steps of a
workflow land on **the same partition index** — with 12 partitions that is
the same index in every topic.

It follows that:

* A sequential workflow runs on **one** instance, namely the one that
  holds the matching partitions. It is not distributed across both.
* Distribution happens across workflows. With two consecutive runs
  of the same definition, it can happen that both land on the same instance —
  with only a few workflows that is not a bug, but the hash.
* Anyone who needs parallelism **within** a workflow must build parallel branches
  in the DAG. Those then run as a batch (up to `max.poll.records`, default 8)
  simultaneously — but still on the instance that holds their partition.
* If an instance fails, throughput is halved; workflows continue that
  sit on the remaining partition.

`WORKER_MAX_CONCURRENT_TASKS` is **per instance**. The value becomes
effective doubled, which is why it is set explicitly in Compose instead of
remaining at the worker default of 24.

### Two dedicated services instead of `--scale`

Each instance needs its own published port for health and metrics.
With `--scale quad-worker=2`, a fixed port mapping collides on the second
replica. For larger fleets: remove the port mapping and
use `docker compose up -d --scale quad-worker=2` — then
health and metrics are only reachable via the orchestrator or in the cluster.

## Health and metrics

| Purpose | Endpoint |
|---|---|
| Health | `GET /actuator/health` |
| Metrics | `GET /actuator/prometheus` |
| Workflow status | `GET /api/v1/workflows/{executionId}` |
| HITL tickets | `GET /api/v1/hitl/tickets` |

Relevant for operations: `orchestrator.stale_result_total` counts results
that belong to a task that is no longer current. A permanently
rising value means that workers deliver results that the orchestrator
discards — typical after a rebalance.

## Still open before a delivery

`app.security.enabled` is set to `false`, the JWT secret has a
placeholder default and `POSTGRES_PASSWORD` a default value. All endpoints
of this platform are therefore reachable without authentication as soon as the port
is reachable. For operation with network access, this must be decided; the
folder deliberately contains no security configuration.
