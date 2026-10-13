# Delivery

Auslieferungsordner für beide Plattformen. Die Gliederung folgt der Laufzeit, die
tatsächlich deployed wird, und der Anwendung dahinter.

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

## Zuordnung

| Ordner | Deployt | Quelle |
|---|---|---|
| `quarkus/quad` | quad-backend, quad-ui, MCP-Server | `quad-quarkus/docker`, `quad-quarkus/deploy`, `quad-quarkus/scripts` |
| `quarkus/kafka` | Kafka, Postgres-pgvector, Redis für den Quarkus-Stack | `quad-quarkus/deploy/01-infrastructure.yml`, `kafka-platform/kafka`, `kafka-platform/scripts` |
| `spring/quad` | quad-workflow-worker | `kafka-platform/docker/Dockerfile-quad-workflow-worker*` |
| `spring/kafka` | agent-orchestrator, agent-worker, kompletter Stack | `kafka-platform/docker`, `docker-compose*.yml`, `helm`, `grafana`, `prometheus` |

## Zwei Wege, ein Ziel

Ein Workflow, der in quad entworfen wurde, wird auf der Kafka-Plattform
ausgeführt. Dafür werden beide Laufzeiten gebraucht, aber nicht zwingend beide
installiert:

**Nur Kafka-Plattform** — die eigenständige Variante. `spring/kafka` plus
`spring/quad` für den Worker. Der Workflow wird über
`POST /api/v1/unified/workflows/from-quad` übernommen, die Agenten über
`POST /api/v1/unified/agents/convert`. Das ausführbare Beispiel liegt in
`kafka-platform/examples/quad-to-kafka/` mit README.

**Quad plus Plattform** — wenn quad selbst auch laufen soll (eigene Workflows,
quad-ui, MCP). Dann zusätzlich `quarkus/quad` und `quarkus/kafka`.

## Erzeugen und erneuern

Die Ordner werden nicht von Hand gepflegt, sondern erzeugt:

```bash
cd delivery && ./sync-from-source.sh              # erneuert die Kopien
cd delivery && ./sync-from-source.sh --clean      # leert vorher deployment/ und config/
```

Das Mapping steht vollständig im Skript. Nach einer Änderung an einem Dockerfile,
Compose-File oder Chart genügt ein Lauf — Ordner und Skript können nicht
auseinanderlaufen, weil es nur eine Quelle für das Mapping gibt.

**Welche Ordner generiert sind und welche nicht:**

| Pfad | Herkunft |
|---|---|
| `*/deployment/`, `*/config/` | generiert — vollständig aus `sync-from-source.sh`, nicht von Hand ändern |
| `README.md` (alle Ebenen) | von Hand gepflegt, nicht Teil des Mappings |

Deshalb **kein `rm -rf quarkus spring`**. Das Skript schreibt additiv und lässt
die README-Dateien stehen; `--clean` entfernt gezielt nur die generierten
Ordner. `--clean` behebt zugleich das Drift-Problem eines additiven Laufs: eine
im Quell-Repo gelöschte Datei bleibt sonst als veraltete Kopie stehen.

Das Skript kopiert **ausschließlich git-getrackte Dateien**. In beiden Repos sind
`.env`, `.env.prod`, `.env.secrets.cloud` und `docker/secrets/` ignoriert und
werden deshalb übersprungen — mit Absicht: eine Delivery wird weitergegeben, und
ein einmal mitgelieferter Schlüssel ist aus dem Working Tree nicht mehr zu
entfernen. Für Konfiguration liefern die `config`-Ordner deshalb Templates mit
Variablenverweisen (`${PROD_DB_PASSWORD}`), die beim Zielsystem zu setzen sind.

Vor dem ersten Deployment zu setzen:

| Variable | Wo | Warum |
|---|---|---|
| `WORKER_QUAD_AGENT_SOURCE=db` | `spring/quad` | Ohne das kennt der Worker nur die statische YAML-Liste und meldet `Unknown agent type` für jeden über `/agents/convert` importierten Agenten. |
| `OPENAI_API_KEY` | `spring/quad` | Ohne Key fällt der Worker auf ein Mock-Modell zurück. |
| `modelId` der Agenten | `spring/quad` | Muss `simple` sein — der Worker registriert genau diesen einen Key. |
| `EXTERNAL_KAFKA_BOOTSTRAP_SERVERS`, `POSTGRES_URL` | `spring/kafka` | Pflicht für `docker-compose.minimal.yml`, wird beim Start geprüft. |
| `postgresqlPassword` | `spring/kafka/config/helm` | Chart-Default ist ein fester Wert und gehört ersetzt. |

## Bekannte Grenzen

- **Geteilte Dateien.** `docker-compose.minimal.yml` und `start-minimal.sh`
  starten Orchestrator *und* Worker und liegen deshalb vollständig unter
  `spring/kafka`. Für den Worker alone sind in `spring/quad/deployment/docker`
  die Dockerfiles hinterlegt.
- **Verweise auf Quell-Repos.** Compose-Overlays und Skripte erwarten Pfade
  innerhalb des jeweiligen Repos (`../quad-core`, Maven-Module). Sie sind als
  Referenz kopiert, nicht als eigenständiges Build-Artefakt — der Build läuft im
  jeweiligen Repo.
- **Helm-Charts sind kopiert, nicht angepasst.** `deployment/helm` ist der
  Chart-Stand aus dem Quell-Repo. Anpassungen für das Zielsystem gehören in ein
  Overlay unter `config/helm`, nicht in den Chart.
- **Sicherheit nicht enthalten.** Authentifizierung ist in beiden Plattformen
  per Default aus (`app.security.enabled=false`). Vor einer Auslieferung mit
  Netzzugang ist das zu entscheiden — siehe Repo-Root von `kafka-platform`.
