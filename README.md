# Agentic Platform — vom quad-Agenten zur dauerhaften Workflow-Plattform

Ein Monorepo mit **einer gemeinsamen Agenten-Basis** (`quad-core`) und **drei Stufen** der
Workflow-Orchestrierung:

```
   Stufe 1                Stufe 2                       Stufe 3
   quad-quarkus           kafka-platform                agentflow
   synchron / in-process  asynchron / event-getrieben   dauerhaft / durabel
   (Quarkus)              (Kafka)                       (Temporal)
        │                      │                             │
        └──────────────────────┴─────────────────────────────┘
                               │
                    gemeinsame Basis: quad-core
              (LangChain4j-Agent, ReAct, Hooks, Tools, MCP,
               Guardrails, PII, HITL, Observability)
```

Die Agenten selbst ändern sich über die Stufen **nicht** — es ändert sich nur, *wer* die
Ausführung orchestriert und *wo* die Dauerhaftigkeit (Retries, Checkpoints, Replay) liegt.

---

## Die gemeinsame Basis: `quad-core`

`quad-core` (in `quad-quarkus/`) ist ein **framework-neutrales Java-Framework für LLM-Agenten
auf Basis von LangChain4j**. Es ist die Konstante in allen drei Stufen: ein Agent wird immer
gleich gebaut, unabhängig davon, ob er in Quarkus, in einem Kafka-Worker oder in einer
Temporal-Activity läuft.

```java
Agent agent = AgentBuilder.defaultAgent()
        .withModel(ModelFactory.createOpenAiFromEnv())   // OPENAI_*
        .withSystemPrompt("Du bist ein Recherche-Assistent.")
        .withToolObjects(new ReadFileTool(), new WebSearchTool())
        .withGuardrail(promptInjectionGuardrail())
        .withHook(piiHook())                              // GDPR/PII-Maskierung
        .withHook(new AuditTrailHook(store))
        .withEventPublisher(publisher)                    // Metriken/Tracing/Logging
        .build();

String answer = agent.run(task, new AgentSessionState());
```

Was `quad-core` liefert (Auszug):

| Baustein | Bedeutung |
|---|---|
| `AgentBuilder` / `AgentRuntime` | Agent-Assemblierung + ReAct-Loop (Reason → Act → Observe) |
| **Hook-Pipeline** | 6 Eingriffspunkte (`before/after Agent/Model/Tool`) → Guardrails, HITL, Telemetrie, Resilience, GDPR als Steckmodule |
| Tools & MCP | Built-ins (`readFile`, `grepSearch`, `webSearch`, …), `@Tool`-Methoden, `SubAgentTool`, MCP-Bridge |
| Guardrails / PII / Audit | `GuardrailPlugin`, `PiiAnonymizerHook`, `AuditTrailHook` |
| HITL | `HITLPlugin` + `CheckpointService` (SSE/Kafka/Console-Channels) |
| Observability | typisierte `AgentEvent`s, `MetricsHook` (Micrometer), `TracingHook` (OTel), `ProvenanceTracker` |
| Modell | `ModelFactory` (OpenAI-kompatibel, `OPENAI_*`/`QUAD_LLM_*`) |

**Wer `quad-core` einbindet:**

| Modul | quad-Abhängigkeit |
|---|---|
| `quad-quarkus/quad-quarkus` | `quad-core` (direkt, in-process) |
| `kafka-platform/quad-workflow-worker` | `de.augmentia.quad:quad-core` + `quad-tool-builtin` |
| `agentflow/agentflow-app` | `quad-core` + `quad-tool-builtin` + `quad-tool-mcp` |

> Der Root-Aggregator löst `quad-core` **im Reaktor** auf, damit der Kafka-Worker nicht ein
> veraltetes Jar aus `~/.m2` zieht (siehe [Build & Test](#build--test)).

---

## Der Weg in drei Stufen

### Stufe 1 — `quad-quarkus`: synchron, in-process (Quarkus + LangChain4j)

Der Agent läuft **im selben Prozess** wie die Anwendung. Quarkus stellt REST/SSE und die
React-UI (`quad-ui`); die Workflow-Engine (`quad.workflow` v2) führt DAGs mit 7 Node-Executoren
(`agent`, `decision`, `loop`, `fork`, `join`, `hitl`, `end`) aus. Asynchronität entsteht nur dort,
wo man sie explizit will — über Messaging-Channels (Kafka/AMQP/E-Mail).

```
Client ──REST/SSE──► quad-quarkus (Quarkus)
                        │  AgentBuilder + AgentRuntime + Hook-Pipeline   (quad-core)
                        │  Workflow-Engine (quad.workflow v2)
                        └─ JDBC-Stores (SQLite/Postgres): runs, memory, audit, skills, checkpoints
```

- **Passt zu:** interaktiven, agent-zentrierten Anwendungen mit UI, kurzen Läufen, direkter
  Kontrolle über Prompt/Tools.
- **Grenze:** Orchestrierung und Dauerhaftigkeit liegen im Anwendungsprozess; Skalierung und
  Lastverteilung eines Laufs über mehrere Instanzen sind Handarbeit.
- **Start:** `cd quad-quarkus && mvn quarkus:dev -Dquarkus.http.port=8083` (UI: `quad-ui`).
- Details: [`quad-quarkus/README.md`](quad-quarkus/README.md).

### Stufe 2 — `kafka-platform`: asynchron, event-getrieben (Kafka)

Die **Orchestrierung wird vom Agenten entkoppelt**. Ein deterministischer Orchestrator hält den
Routing-Zustand; die eigentliche Agenten-Arbeit läuft als **Task über Kafka** und kommt als
**Result** zurück. Derselbe `quad-core`-Agent steckt jetzt in `quad-workflow-worker` — die
übrigen Module (`agent-common`, `agent-orchestrator`, `agent-worker`) bleiben bewusst
**LLM-frei und deterministisch**.

```
        quad-Workflow/Agent  ──/unified/…──►  Bridge  ──►  workflow-/agent_definitions (Postgres)
                                                            │
 Orchestrator (Spring Boot) ◄──intake───────────────────────┘
   │  RoutingEvaluator (pur, deterministisch, replaybar)
   │  Checkpoints · Retries · HITL-Tickets · Sweep · Redis-Idempotenz
   ▼
 agent.tasks.<agentType>   (Key = workflow_id)
   │
   ▼
 Worker:  ┌ agent-worker            (deterministischer Mock)
          └ quad-workflow-worker    (quad-core + echter LLM, Mock-Fallback)
   │
   ▼
 agent.execution.results ──► Orchestrator ──► nächster Schritt … ──► END
```

- **Neu gegenüber Stufe 1:** Dauerhaftigkeit liegt in **Postgres** (Checkpoints), **Redis**
  (Start-Idempotenz) und Kafka (Retention/Replay); Lastverteilung über Partitionen
  (`Key = workflow_id`) und Worker-Instanzen; HITL als entkoppelte Tickets.
- **Bridge (quad → kafka):** `POST /api/v1/unified/agents/convert` (Agenten zuerst!) und
  `POST /api/v1/unified/workflows/from-quad` (Workflow). Details:
  [`kafka-platform/examples/quad-to-kafka/README.md`](kafka-platform/examples/quad-to-kafka/README.md).
- **Passt zu:** hohem Durchsatz, Parallelität, loser Kopplung, mehreren Worker-Flotten.
- **Grenze:** Orchestrierungslogik (Retries, Timers, Zustandsmaschine) ist **selbst gebaut**
  und muss deterministisch/replaybar gehalten werden.
- **Start:** `cd kafka-platform && mvn verify`, dann `docker compose up -d kafka postgres redis`.
- Details: [`kafka-platform/README.md`](kafka-platform/README.md).

### Stufe 3 — `agentflow`: dauerhaft, durabel (Temporal + Spring AI)

Hier ersetzt **Temporal** die handgebaute Kafka-Orchestrierung. Ein Workflow ist
deterministischer Code; alles Nicht-Deterministische (LLM-/Tool-Aufrufe) läuft in **Activities**.
Dieselben `quad-core`-Agenten werden als Activity gebaut und ausgeführt; zusätzlich gibt es einen
Spring-AI-Pfad (LangGraph4j) und **deklarative `quad.workflow`-v2-Dokumente**, die ein generischer
Runner über alle Node-Typen (`agent`, `decision`, `loop`, `fork`, `join`, `hitl`, `subworkflow`,
`end`) ausführt.

```
Client ──REST──► agentflow-app (Spring Boot)
                    │  startet Workflow  ──►  Temporal Server (:7233)
                    │                              │  Event-History = Durability
                    │   ┌──────────────────────────┘  Replay · Retries · Timer · Signals
                    │   ▼
                    │  Workflow (deterministisch): Plan/DAG, Verzweigung, HITL-Wait
                    │      │  Activity
                    │      ▼
                    └─  Activity: quad-core-Agent / Spring AI / Tools / MCP  (nicht-deterministisch)
```

- **Neu gegenüber Stufe 2:** Durability, Retries, Timers und HITL-**Signale** sind
  **Plattform-Features** statt Eigenbau. Die Event-History erlaubt deterministisches Replay;
  ein Worker-Neustart ist unkritisch. Deklarative v2-Definitionen werden validiert
  (genau ein `end`, azyklisch, auflösbare Referenzen) und **fail-closed** publiziert.
- **Passt zu:** lang laufenden, ausfallsicheren Geschäftsprozessen mit menschlichen
  Freigaben und reproduzierbarem Audit.
- **Starten:** `cd agentflow && ./start-agentflow.sh` (App `:8095`, Temporal-UI `:8233`, Angular-UI `:4200`).
- Details: [`agentflow/README.md`](agentflow/README.md).

---

## Was sich pro Stufe ändert (und was nicht)

| Aspekt | Stufe 1 · quad-quarkus | Stufe 2 · kafka-platform | Stufe 3 · agentflow |
|---|---|---|---|
| **Agent-Basis** | `quad-core` (LangChain4j) | `quad-core` (in `quad-workflow-worker`) | `quad-core` (+ Spring AI) |
| **Orchestrierung** | in-process (Workflow-Engine) | Kafka-Choreografie + deterministischer Evaluator | **Temporal** |
| **Durability** | DB-Stores/Checkpoints im Prozess | Postgres-Checkpoints, Redis-Idempotenz, Sweep | **Temporal-History (Replay)** |
| **Trigger** | REST/SSE, Messaging-Channels | Kafka-Topics | REST/SSE + Temporal-Signals |
| **Modellaufruf** | in-process | im Kafka-Worker | in Temporal-Activities |
| **HITL** | Checkpoints via SSE | HITL-Tickets (Kafka) | Temporal-Signals |
| **UI** | React (`quad-ui`) | REST-API (+ Brücke aus `quad-ui`) | Angular (`agentflow-ui`) |
| **LLM-frei?** | nein | Kern ja (`agent-common`/`orchestrator`/`worker`), LLM nur im `quad-workflow-worker` | nein (LLM in Activities gekapselt) |

**Leitlinie über alle Stufen:** *durable/Graph-Grenzen explizit halten, nicht-idempotente
Effekte nie ohne Key retryen, Checkpoints schlank halten* — bei jeder Stufe bleibt der Agent
gleich, nur die Orchestrierung wandert nach außen.

---

## Projektstruktur

```text
.
├── pom.xml               # Maven-Aggregator (quad-quarkus, kafka-platform, agentflow)
├── README.md             # diese Datei
├── .gitignore            # konsolidiert aus allen Modulen
├── quad-quarkus/         # QUAD – Agent-Framework (Quarkus + LangChain4j) + React-UI
├── kafka-platform/       # Event-getriebene Workflow-Plattform (Kafka + quad-core-Worker)
├── agentflow/            # Durable Agent-Workflows (LangGraph4j + Spring AI + Temporal) + Angular-UI
└── integration-tests/    # End-to-End-Tests über die Plattform (eigener Reaktor)
```

Root-Ebene trackt nur `README.md`, `pom.xml` und `.gitignore`; lokale Hilfsdateien (Makefile,
`start-*.sh`, Notizen, `.code-workspace`) sind ignoriert.

---

## Build & Test

Der Root-Aggregator löst `quad-core` aus dem **Reaktor** auf, damit
`kafka-platform/quad-workflow-worker` nicht ein veraltetes Jar aus `~/.m2` zieht:

```bash
# alles (korrekte Reihenfolge, inkl. Tests)
mvn -T1C clean install

# einzelne Reaktoren
cd quad-quarkus    && mvn clean install          # quad-core + Quarkus + UI   (~500 + ~200 Tests)
cd kafka-platform  && mvn verify                 # Orchestrator/Worker/quad-workflow-worker (~360 Tests)
cd agentflow       && mvn clean package          # App + UI (quad-core aus ~/.m2)

# Integrationsprojekt (eigener Reaktor, eigene Ports)
cd integration-tests && ./scripts/run-integration-tests.sh e2e   # L0–L3, 58 Tests
```

> **Stale-Jar-Trap:** Nach Änderungen an `quad-core` in den Konsumenten immer
> `mvn … clean package` bzw. `mvn install` — ohne `clean` wird die alte `quad-core` ins
> Fat-Jar gebündelt.

**Frontends**

```bash
cd quad-quarkus/quad-ui    && npm install && npm run build
cd agentflow/agentflow-ui  && npm install && npm run build
```

---

## Ports (lokaler Stack)

| Komponente | Port |
|---|---|
| quad-quarkus (Quarkus) | `8083` (dev) / `8086` (Default) |
| quad-ui (React) | `5173` (Vite dev) |
| agent-orchestrator (Kafka) | `8080` |
| quad-workflow-worker | `8092` |
| Kafka / Postgres / Redis | `9092` / `5432` / `6379` |
| agentflow-app | `8095` |
| Temporal (gRPC / UI) | `7233` / `8233` |
| agentflow-ui (Angular) | `4200` |

---

## Weiterführend

| Thema | Dokument |
|---|---|
| Agent-Framework im Detail (Hooks, Guardrails, HITL, PII, Security, Deployment) | [`quad-quarkus/README.md`](quad-quarkus/README.md) |
| Kafka-Plattform (Topics, Routing, HITL-Tickets, Mock-Verhalten) | [`kafka-platform/README.md`](kafka-platform/README.md) |
| quad-Workflow → Kafka ausführen (Bridge, Konvertierung) | [`kafka-platform/examples/quad-to-kafka/README.md`](kafka-platform/examples/quad-to-kafka/README.md) |
| agentflow (Temporal, deklarative v2-Workflows, Guardrails pro Agent) | [`agentflow/README.md`](agentflow/README.md) |
| End-to-End-Schichten L0–L3 + dokumentierte Befunde | [`integration-tests/README.md`](integration-tests/README.md) |

---

## Hinweise

- **Neue Historie:** Dieses Monorepo wurde aus den vormals getrennten Repositories `quad`,
  `kafka-platform` und `agentflow` mit **neuer Historie** zusammengeführt; die alten Repositories
  bleiben als Historie/Backup bestehen.
- **Secrets:** nur `*.example`-Templates sind getrackt; echte `.env`-Dateien sind ignoriert.
