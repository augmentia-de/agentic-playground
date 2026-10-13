# Spring-Boot-Laufzeit

Beide Dienste der Kafka-Plattform laufen auf Spring Boot. Der Worker bringt
quad-core mit und führt Workflow-Schritte aus, die quad entworfen hat.

## `quad/` — quad-workflow-worker

Führt einzelne Workflow-Schritte aus. Nimmt Tasks von `agent.tasks.*` und legt
das Ergebnis auf `agent.execution.results`.

```
deployment/
├── docker/
│   ├── Dockerfile-quad-workflow-worker          Build mit Toolchain
│   └── Dockerfile-quad-workflow-worker-runtime  Runtime-only, ohne Build-Step
├── scripts/build-deps-tarball.sh               Maven-Abhängigkeiten als Tarball
config/
└── worker.env.example                          erforderliche Einstellungen
```

Das Compose-Fragment für den Worker liegt in `../kafka/deployment/compose/`,
weil `docker-compose.minimal.yml` beide Dienste gemeinsam startet.

### Zwei Einstellungen ohne die nichts aus importierten Agenten läuft

**`WORKER_QUAD_AGENT_SOURCE=db`.** Der Worker liest Agentendefinitionen nur bei
dieser Einstellung aus der Datenbank. Sonst greift die statische Liste aus
`application.yml` und jeder über `/agents/convert` importierte Agent scheitert:

```
Unknown agent type: ex-market-scout. Available types: coder, fixer, linter, …
```

**`modelId` muss `simple` sein.** Der Worker registriert genau einen Model-Key,
aus `OPENAI_*` in `ChatModelRegistry`. Jeder andere Key wirft:

```
IllegalArgumentException: Unknown model key: gpt-4o-mini. Available: [simple]
```

Das betrifft auch den automatisch angelegten Stub-Agenten. Der Orchestrator
schreibt seinen Default aus `app.agents.default-model-key` (Default `simple`),
das muss zum registrierten Key passen.

### Timeout-Grenzen

Der Listener wartet auf den **kompletten** Batch, bevor der nächste Poll
startet. Daraus folgt die Kette:

```
worker.quad.max-task-timeout-ms   (600000)
  < max.poll.interval.ms          (900000)
  < .env-Datei / Workflow, das einen Timeout > 600s fordert
```

Ein Task über 600 s wird gekürzt und protokolliert. Ohne diese Grenze evictet
ein einzelner Task den Consumer, die Partition wird neu zugestellt und alle
LLM-Aufrufe der Partition werden zweimal bezahlt.

`auto.offset.reset` steht auf `earliest`. Auf `latest` fällt ein Task, der vor
der Partition-Zuweisung produziert wurde — bei jedem neu importierten Agent-Typ
der erste Task. Der Preis ist ein Replay beim Beitritt zu einer Gruppe, den der
Result-Cache ohne zweiten LLM-Call bedient.
