# Quickstart: Creating Agents in 5 Steps

Five examples that build on one another show how to create an agent — from the
one-liner to the composed system. They live in
`de.augmentia.quad.examples.quickstart` and use exclusively the current API
(`Agents`, `AgentBuilder`, `AgentConfig`).

| Step | File | New |
|---|---|---|
| 1 | `Step1_MinimalAgent` | Getting started: `Agents.simple().run(...)` — no custom class, no config |
| 2 | `Step2_AgentWithTool` | Custom `Agent` class + `@Tool`; `AgentBuilder.create(...)` |
| 3 | `Step3_ConfiguredAgent` | `AgentConfig` (prompt, sampling, limits) + tools as objects, without subclassing |
| 4 | `Step4_StructuredOutput` | Type-safe output via a `record` + `executeStructured(...)` |
| 5 | `Step5_SubAgents` | Composition: orchestrator delegates via `withSubAgent(...)` |

Each class has a `main` method and a header comment "New compared to step N-1".
All of them require `OPENAI_API_KEY` (optionally `OPENAI_MODEL`) from the environment.

## Running

From the IDE: start the respective `main` method.

With Maven (example step 1):

```bash
mvn -pl quad-examples -am compile
mvn -pl quad-examples exec:java \
  -Dexec.mainClass=de.augmentia.quad.examples.quickstart.Step1_MinimalAgent
```

With the script (loads the LLM settings from `quad-quarkus/.env`):

```bash
quad-examples/run-step1.sh              # Schritt 1 (MinimalAgent)
quad-examples/run-step1.sh --step 3     # anderer Schritt (1..5)
quad-examples/run-step1.sh --keys       # zusätzlich set_keys.sh sourcen (überschreibt OPENAI_*)
quad-examples/run-step1.sh --rebuild    # Reactor-Abhängigkeiten frisch installieren
quad-examples/run-step1.sh --list       # verfügbare Schritte
```

The script loads `.env` (via `set -a; source`), compiles `quad-examples` and starts the
selected `main` class via the Java classpath. A different `.env` via `QUAD_ENV_FILE=/pfad/.env`.

## Scope

`quad-examples` additionally contains a grown collection for individual features
(`features/`), workflows/orchestration (`orchestrator/`, `TwoStageWorkflow*`),
capability/MCP (`Capability*`) and diagnostic probes (`PrefixProbeExample`). The
`quickstart/Step1..5` are intended for learning how to create agents; the remaining
examples remain as a reference for special topics.
