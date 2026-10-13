# quad-workflow-worker

Führt einzelne Schritte eines quad-Workflows aus. Nimmt Tasks von
`agent.tasks.<agentType>` und schreibt das Ergebnis auf
`agent.execution.results`.

## Inhalt

```
deployment/
├── docker/
│   ├── Dockerfile-quad-workflow-worker          Build inklusive Toolchain
│   └── Dockerfile-quad-workflow-worker-runtime  Runtime-only, ohne Build-Step
└── scripts/build-deps-tarball.sh               Maven-Abhängigkeiten als Tarball
config/
└── worker.env.example                          erforderliche Einstellungen
```

Das Compose-Fragment mit dem Worker-Service liegt in
`../kafka/deployment/compose/docker-compose.minimal.yml`, weil
`docker-compose.minimal.yml` Orchestrator und Worker gemeinsam startet und
nicht sinnvoll zu trennen ist.

## Voraussetzungen

Der Worker braucht Datenbankzugriff auf dieselbe `agent_definitions`, in die
der Orchestrator schreibt. Ohne `WORKER_QUAD_AGENT_SOURCE=db` ignoriert er die
Tabelle vollständig und kennt nur die statisch konfigurierten Agenten aus
`application.yml`.

Beispielstart:

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

## Vor dem ersten Task prüfen

1. **Agent-Key stimmt.** Der Worker registriert genau einen Model-Key: `simple`.
   Der `modelId` jedes importierten Agenten muss genau dieser sein.
2. **Topic existiert mit 12 Partitionen.** Der Orchestrator legt es beim Import
   an. Ein vom Broker automatisch angelegtes Topic hätte nur eine Partition und
   damit streng serielle Verarbeitung:

   ```bash
   kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic agent.tasks.<agentType> | wc -l   # erwartet 12
   ```
3. **Task-Timeout passt in die Poll-Grenze.** Siehe `../README.md`.
