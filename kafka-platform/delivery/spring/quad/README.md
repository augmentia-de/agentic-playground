# quad-workflow-worker

Executes individual steps of a quad workflow. Takes tasks from
`agent.tasks.<agentType>` and writes the result to
`agent.execution.results`.

## Contents

```
deployment/
├── docker/
│   ├── Dockerfile-quad-workflow-worker          Build inklusive Toolchain
│   └── Dockerfile-quad-workflow-worker-runtime  Runtime-only, ohne Build-Step
└── scripts/build-deps-tarball.sh               Maven-Abhängigkeiten als Tarball
config/
└── worker.env.example                          erforderliche Einstellungen
```

The Compose fragment with the worker service is in
`../kafka/deployment/compose/docker-compose.minimal.yml`, because
`docker-compose.minimal.yml` starts the orchestrator and worker together and
cannot sensibly be separated.

## Prerequisites

The worker needs database access to the same `agent_definitions` that
the orchestrator writes to. Without `WORKER_QUAD_AGENT_SOURCE=db` it ignores the
table completely and only knows the statically configured agents from
`application.yml`.

Example start:

```bash
docker build -f deployment/docker/Dockerfile-quad-workflow-worker-runtime -t quad-worker:1.0.0 .

docker run --rm \
  -e WORKER_QUAD_AGENT_SOURCE=db \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/agent_platform \
  -e SPRING_DATASOURCE_USERNAME=agent \
  -e SPRING_DATASOURCE_PASSWORD="$PG_PASSWORD" \
  -e OPENAI_API_KEY="$OPENAI_API_KEY" \
  -e OPENAI_BASE_URL="$OPENAI_BASE_URL" \
  -e OPENAI_MODEL="$OPENAI_MODEL" \
  -p 8092:8092 \
  quad-worker:1.0.0
```

## Check before the first task

1. **Agent key matches.** The worker registers exactly one model key: `simple`.
   The `modelId` of every imported agent must be exactly this one.
2. **Topic exists with 12 partitions.** The orchestrator creates it during import.
   A topic created automatically by the broker would have only one partition and
   therefore strictly serial processing:

   ```bash
   kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic agent.tasks.<agentType> | wc -l   # erwartet 12
   ```
3. **Task timeout fits within the poll limit.** See `../README.md`.
