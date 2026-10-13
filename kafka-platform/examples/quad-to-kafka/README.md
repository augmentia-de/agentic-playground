# Run a workflow from quad over Kafka

A workflow designed in quad can be run on this platform without redrawing it.
This directory contains a complete, runnable three-step workflow plus everything
needed to reproduce it.

## The flow

```
quad-Workflow (flach: {name, nodes[], edges[]})
   │
   │  1. je Agent:  POST /api/v1/unified/agents/convert
   │                 → agent_definitions (system_prompt, model, tools)
   │
   │  2. Workflow:   POST /api/v1/unified/workflows/from-quad
   │                 → WorkflowDefinitionBridge.convert()
   │                 → workflow_definitions
   │                 → Task-Topics mit 12 Partitionen
   │                 → agent.execution.intake
   ▼
  WorkflowEngine  ──►  agent.tasks.<agentType>   (Key = workflow_id)
   ▲                          │
   │                          ▼
   │                  Worker: ChatModel + System-Prompt aus der DB
   │                          │
   └──  agent.execution.results ◄┘
```

## Two formats, one field decides

The bridge accepts both quad formats. What matters is **where** the
attributes live:

| | quad-UI | quad WorkflowGeneratorService |
|---|---|---|
| Attributes | `properties` | `config` |
| Agent binding | `agentType` (top-level) | `config.agentId` |

The bridge reads a merged view (`config` as the base, `properties` wins
on conflict) and resolves the agent type like this:

```
agentType  →  properties/config: agentType  →  agentId  →  agent_id  →  "generic" (mit Warnung)
```

`config.agentId` is exactly the right value here: `AgentDefinitionBridge`
registers `quadAgent.id()` as `agent_type`, and the worker resolves agents via
`WHERE agent_type = ?`.

## Order is not optional

1. **Agents first.** `/from-quad` registers missing agents only as a stub
   (generic prompt, no tools). A stub is a stopgap, not a replacement
   for the conversion.
2. **Then the workflow.** During import, the task topics of the referenced
   agent types are created with the configured partition count.

## Prerequisites in the worker

Two settings without which imported agents do not run:

| Variable | Value | Why |
|---|---|---|
| `WORKER_QUAD_AGENT_SOURCE` | `db` | Otherwise the worker only knows the static YAML list and reports `Unknown agent type` for everything that came via `/agents/convert`. |
| `OPENAI_API_KEY` | set | Without a key the worker falls back to a mock model. |

`modelId` in `agents.json` must be a key that the worker has registered.
The worker registers exactly **one**: `simple` (from `OPENAI_*`). Anything else
fails with `Unknown model key`.

## Running

```bash
./run.sh                       # workflow.json mit Default-Aufgabe
./run.sh mein-workflow.json "Andere Aufgabenstellung"
ORCH=http://localhost:8080 ./run.sh
```

The script converts the three agents, starts the workflow, waits for
completion and prints the checkpoints with the responses.

## Things to watch out for

**The workflow name is the identity.** A second POST with the same name
returns the already stored definition (`duplicate: true`) — a
corrected version is ignored. For every changed draft use a new
name or delete the definition.

**Retry.** `/from-quad` starts immediately. On timeout, resend with the
returned `idempotencyKey`, then the same execution comes
back instead of a second one. Alternatively without auto-start:
`POST /api/v1/workflow-definitions` (store only) and afterwards
`POST /api/v1/workflows/start` with your own key.

**Timeouts.** `config.timeoutSeconds` lands in the task as `timeoutMs`. The
worker caps at `worker.quad.max-task-timeout-ms` (600 s) — it must stay below
`max.poll.interval.ms` (900 s), because the listener blocks the poll thread until
the end of the batch.

**Concurrency.** Tasks are keyed by `workflow_id`, all steps of a
workflow land on one partition. Parallel branches run only within
a batch (`max.poll.records`, default 8); the limit for parallel workflows
is partition count × worker instances.

**What is lost during import.** The response to `/agents/convert` names it:
skills registry, named hooks, directory permissions and MCP/sandbox support
do not exist in the Kafka worker.

## The concrete workflow

Three steps, each with its own agent and its own system prompt, as a chain:

```
node-Scout  (ex-market-scout)   → node-Analyst (ex-risk-analyst) → node-Writer (ex-report-writer)
```

Measured run (`space-bunny-free`, ~3 LLM calls):

```
status: COMPLETED
  n3-Scout     ex3-scout        300000  COMPLETED  → "- EU rules allow qualifying e-cargo bikes up to 1,000 kg…"
  n3-Analyst   ex3-analyst      300000  COMPLETED  → "1. Regulatory divergence or tightening could restrict…"
  n3-Writer    ex3-writer       300000  COMPLETED  → "Cargo bikes in European cities have a positive but uneven outlook…"
  __end                          60000  COMPLETED
```

Each step sees the previous step's response — the chaining is real
data passing, not just an ordering of nodes.
