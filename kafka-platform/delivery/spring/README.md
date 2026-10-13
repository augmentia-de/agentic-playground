# Spring Boot runtime

Both services of the Kafka platform run on Spring Boot. The worker bundles
quad-core and executes workflow steps that quad designed.

## `quad/` — quad-workflow-worker

Executes individual workflow steps. Takes tasks from `agent.tasks.*` and puts
the result on `agent.execution.results`.

```
deployment/
├── docker/
│   ├── Dockerfile-quad-workflow-worker          Build mit Toolchain
│   └── Dockerfile-quad-workflow-worker-runtime  Runtime-only, ohne Build-Step
├── scripts/build-deps-tarball.sh               Maven-Abhängigkeiten als Tarball
config/
└── worker.env.example                          erforderliche Einstellungen
```

The Compose fragment for the worker is in `../kafka/deployment/compose/`,
because `docker-compose.minimal.yml` starts both services together.

### Two settings without which nothing from imported agents runs

**`WORKER_QUAD_AGENT_SOURCE=db`.** The worker reads agent definitions from the
database only with this setting. Otherwise the static list from
`application.yml` applies and every agent imported via `/agents/convert` fails:

```
Unknown agent type: ex-market-scout. Available types: coder, fixer, linter, …
```

**`modelId` must be `simple`.** The worker registers exactly one model key,
from `OPENAI_*` in `ChatModelRegistry`. Every other key throws:

```
IllegalArgumentException: Unknown model key: gpt-4o-mini. Available: [simple]
```

This also applies to the automatically created stub agent. The orchestrator
writes its default from `app.agents.default-model-key` (default `simple`),
which must match the registered key.

### Timeout limits

The listener waits for the **complete** batch before the next poll
starts. This gives the chain:

```
worker.quad.max-task-timeout-ms   (600000)
  < max.poll.interval.ms          (900000)
  < .env-Datei / Workflow, das einen Timeout > 600s fordert
```

A task over 600 s is truncated and logged. Without this limit, a single task
evicts the consumer, the partition is redelivered and all
LLM calls of the partition are paid for twice.

`auto.offset.reset` is set to `earliest`. On `latest`, a task produced before
the partition assignment is dropped — for every newly imported agent type
the first task. The price is a replay on joining a group, which the
result cache serves without a second LLM call.
