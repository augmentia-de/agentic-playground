# Workflow aus quad über Kafka ausführen

Ein Workflow, der in quad entworfen wurde, lässt sich auf dieser Plattform
ausführen, ohne ihn neu zu zeichnen. Dieses Verzeichnis enthält einen
vollständigen, lauffähigen Dreischritt-Workflow plus alles, was man zum
Nachvollziehen braucht.

## Der Ablauf

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

## Zwei Formate, ein Feld entscheidet

Die Bridge akzeptiert beide quad-Formate. Entscheidend ist, **wo** die
Attribute stehen:

| | quad-UI | quad WorkflowGeneratorService |
|---|---|---|
| Attribute | `properties` | `config` |
| Agent-Bindung | `agentType` (top-level) | `config.agentId` |

Die Bridge liest eine gemergte Sicht (`config` als Basis, `properties` gewinnt
bei Konflikt) und löst den Agent-Typ so auf:

```
agentType  →  properties/config: agentType  →  agentId  →  agent_id  →  "generic" (mit Warnung)
```

`config.agentId` ist dabei genau der richtige Wert: `AgentDefinitionBridge`
registriert `quadAgent.id()` als `agent_type`, und der Worker löst Agenten über
`WHERE agent_type = ?` auf.

## Reihenfolge ist nicht optional

1. **Agenten zuerst.** `/from-quad` registriert fehlende Agenten nur als Stub
   (generischer Prompt, keine Tools). Ein Stub ist eine Notlösung, kein Ersatz
   für die Konvertierung.
2. **Dann den Workflow.** Beim Import werden die Task-Topics der referenzierten
   Agent-Typen mit der konfigurierten Partitionzahl angelegt.

## Voraussetzungen im Worker

Zwei Einstellungen, ohne die importierte Agenten nicht laufen:

| Variable | Wert | Warum |
|---|---|---|
| `WORKER_QUAD_AGENT_SOURCE` | `db` | Sonst kennt der Worker nur die statische YAML-Liste und meldet `Unknown agent type` für alles, was über `/agents/convert` kam. |
| `OPENAI_API_KEY` | gesetzt | Ohne Key fällt der Worker auf ein Mock-Modell zurück. |

`modelId` in `agents.json` muss ein Key sein, den der Worker registriert hat.
Der Worker registriert genau **einen**: `simple` (aus `OPENAI_*`). Alles andere
scheitert mit `Unknown model key`.

## Ausführen

```bash
./run.sh                       # workflow.json mit Default-Aufgabe
./run.sh mein-workflow.json "Andere Aufgabenstellung"
ORCH=http://localhost:8080 ./run.sh
```

Das Skript konvertiert die drei Agenten, startet den Workflow, wartet auf das
Ende und gibt die Checkpoints mit den Antworten aus.

## Auf was zu achten ist

**Der Workflow-Name ist die Identität.** Ein zweiter POST mit demselben Namen
liefert die bereits gespeicherte Definition zurück (`duplicate: true`) — eine
korrigierte Fassung wird ignoriert. Für jeden geänderten Entwurf einen neuen
Namen verwenden oder die Definition löschen.

**Retry.** `/from-quad` startet sofort mit. Bei Timeout mit dem
zurückgegebenen `idempotencyKey` erneut senden, dann kommt dieselbe Execution
zurück statt einer zweiten. Alternativ ohne Auto-Start:
`POST /api/v1/workflow-definitions` (nur speichern) und danach
`POST /api/v1/workflows/start` mit eigenem Key.

**Timeouts.** `config.timeoutSeconds` landet als `timeoutMs` im Task. Der
Worker kappt bei `worker.quad.max-task-timeout-ms` (600 s) — er muss unter
`max.poll.interval.ms` (900 s) bleiben, weil der Listener den Poll-Thread bis
zum Ende des Batches blockiert.

**Parallelität.** Tasks sind mit `workflow_id` gekeyt, alle Schritte eines
Workflows landen auf einer Partition. Parallele Zweige laufen nur innerhalb
eines Batches (`max.poll.records`, default 8); die Grenze für Workflows
parallel liegt bei Partitionzahl × Worker-Instanzen.

**Was beim Import verloren geht.** Die Antwort auf `/agents/convert` nennt es:
Skills-Registry, benannte Hooks, Verzeichnisrechte und MCP/Sandbox-Unterstützung
gibt es im Kafka-Worker nicht.

## Der konkrete Workflow

Drei Schritte, jeder mit eigenem Agenten und eigenem System-Prompt, als Kette:

```
node-Scout  (ex-market-scout)   → node-Analyst (ex-risk-analyst) → node-Writer (ex-report-writer)
```

Gemessener Lauf (`space-bunny-free`, ~3 LLM-Aufrufe):

```
status: COMPLETED
  n3-Scout     ex3-scout        300000  COMPLETED  → "- EU rules allow qualifying e-cargo bikes up to 1,000 kg…"
  n3-Analyst   ex3-analyst      300000  COMPLETED  → "1. Regulatory divergence or tightening could restrict…"
  n3-Writer    ex3-writer       300000  COMPLETED  → "Cargo bikes in European cities have a positive but uneven outlook…"
  __end                          60000  COMPLETED
```

Jeder Schritt sieht die Antwort des Vorgängers — die Verkettung ist echte
Datenübergabe, nicht nur eine Reihenfolge von Knoten.
