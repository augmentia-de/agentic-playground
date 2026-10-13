# Kafka-Plattform

Orchestrator und Agent-Worker auf Spring Boot. Der Orchestrator nimmt
Workflow-Starts und -Ergebnisse entgegen und steuert den DAG; die Worker führen
einzelne Tasks aus.

## Inhalt

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

## Zwei Startwege

**Vollständiger Stack.** `docker-compose.yml` bringt Kafka, Postgres, Redis,
Vault, Prometheus, Grafana, Jaeger und den OTel-Collector mit.

```bash
cd deployment/compose
docker compose up -d --build
```

**Gegen bestehende Infrastruktur.** `docker-compose.minimal.yml` startet nur
Orchestrator und Worker und bindet Kafka, Postgres und Redis von außen ein.
Beide Variablen sind Pflicht und werden beim Start geprüft:

```bash
export EXTERNAL_KAFKA_BOOTSTRAP_SERVERS=kafka.example.internal:9092
export POSTGRES_URL=jdbc:postgresql://postgres.example.internal:5432/agent_platform
export POSTGRES_USER=agent
export POSTGRES_PASSWORD=...

cd deployment/compose
docker compose -f docker-compose.minimal.yml up -d --build
```

Für den kompletten Lebenszyklus liegt `scripts/start-minimal.sh` bei
(`build`, `start`, `stop`, `status`, `logs`).

## Helm

```bash
helm install agent-platform deployment/helm \
  -f config/helm/values.delivery.yaml \
  -f meine-umgebung.yaml
```

`deployment/helm` ist der Chart-Stand aus dem Quell-Repo und wird nicht
verändert. Zielsystem-Anpassungen gehören in ein Overlay unter `config/helm`.

Der Chart-Default startet Kafka mit einem Replikat und Postgres mit dem
Passwort `agent`. Beides gehört in einer Auslieferung ersetzt, siehe
`config/helm/values.delivery.yaml`.

## Zwei Worker

`compose/docker-compose.two-workers.yml` startet Orchestrator plus **zwei**
`quad-workflow-worker`. Beide teilen sich eine Consumer-Group
(`WORKER_QUAD_AGENT_SOURCE=db`, `WORKER_QUAD_GROUP_ID=quad-workflow-worker`), damit
Kafka die Partitionen aufteilt.

```bash
export EXTERNAL_KAFKA_BOOTSTRAP_SERVERS=kafka.example:9092
export POSTGRES_URL=jdbc:postgresql://postgres.example:5432/agent_platform
export POSTGRES_USER=agent POSTGRES_PASSWORD=...
export OPENAI_API_KEY=... OPENAI_MODEL=...

cd deployment/compose
docker compose -f docker-compose.two-workers.yml up -d --build
```

Host-Ports sind über `ORCH_HOST_PORT`, `WORKER1_HOST_PORT`, `WORKER2_HOST_PORT`
setzbar (Default 8080/8092/8093). Ein festes Port-Mapping kollidiert sonst mit
irgendetwas auf dem Host, und der Fehler erscheint erst beim `up` als
undurchsichtiges `address already in use` aus dem Daemon.

### Gemessene Verteilung

Drei Container, Kafka single-node, 120 Partitionen (10 Agent-Typen × 12):

| Prüfung | Ergebnis |
|---|---|
| Partitionen je Instanz | **60 / 60** |
| davon je Agent-Topic (`ex-market-scout`, `-analyst`, `-writer`) | **6 / 6** |
| 7 Workflows, 18 ausgeführte Tasks | **8 auf worker-1, 12 auf worker-2** |

### Was zwei Worker bringen — und was nicht

Der Durchsatz wächst mit der **Anzahl gleichzeitig laufender Workflows**, nicht
innerhalb eines Workflows. Grund ist der Task-Key: er ist die `workflow_id`, und
alle Agent-Typ-Topics haben 12 Partitionen. Deshalb landen alle Schritte eines
Workflows auf **demselben Partitionsindex** — bei 12 Partitionen also auf
derselben Index in jedem Topic.

Daraus folgt:

* Ein sequenzieller Workflow läuft auf **einer** Instanz, nämlich der, die die
  passenden Partitionen hält. Er ist nicht auf beide verteilt.
* Verteilt wird über Workflows hinweg. Bei zwei aufeinanderfolgenden Läufen
  derselben Definition passiert es, dass beide auf derselben Instanz landen —
  bei nur wenigen Workflows ist das kein Fehler, sondern der Hash.
* Wer Parallelität **innerhalb** eines Workflows braucht, muss parallele Zweige
  im DAG bauen. Die laufen dann als Batch (bis `max.poll.records`, Default 8)
  gleichzeitig — aber weiterhin auf der Instanz, die ihre Partition hält.
* Fällt eine Instanz aus, halbiert sich der Durchsatz; Workflows laufen weiter,
  die auf der verbliebenen Partition sitzen.

`WORKER_MAX_CONCURRENT_TASKS` ist **pro Instanz**. Der Wert wird verdoppelt
wirksam, deshalb ist er im Compose explizit gesetzt statt auf dem Worker-Default
von 24 zu bleiben.

### Zwei eigene Services statt `--scale`

Jede Instanz braucht ihren eigenen veröffentlichten Port für Health und Metriken.
Bei `--scale quad-worker=2` kollidiert ein festes Port-Mapping auf der zweiten
Replika. Für größere Flotten: Port-Mapping entfernen und
`docker compose up -d --scale quad-worker=2` verwenden — dann sind
Health und Metriken nur noch über den Orchestrator bzw. im Cluster erreichbar.

## Health und Metriken

| Zweck | Endpunkt |
|---|---|
| Health | `GET /actuator/health` |
| Metriken | `GET /actuator/prometheus` |
| Workflow-Status | `GET /api/v1/workflows/{executionId}` |
| HITL-Tickets | `GET /api/v1/hitl/tickets` |

Für den Betrieb relevant: `orchestrator.stale_result_total` zählt Ergebnisse,
die zu einem Task gehören, der nicht mehr aktuell ist. Ein dauerhaft
steigender Wert bedeutet, dass Worker Ergebnisse liefern, die der Orchestrator
verwirft — typisch nach einem Rebalance.

## Noch offen vor einer Auslieferung

`app.security.enabled` steht auf `false`, das JWT-Secret hat einen
Platzhalter-Default und `POSTGRES_PASSWORD` einen Default-Wert. Alle Endpunkte
dieser Plattform sind damit ohne Authentifizierung erreichbar, sobald der Port
erreichbar ist. Für einen Betrieb mit Netzzugang ist das zu entscheiden; die
Ablage enthält bewusst keine Security-Konfiguration.
