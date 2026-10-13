# Agentic Platform — Hybrid Monorepo

Ein Repository, drei unabhängige Reaktoren (Quarkus, Spring Boot, Spring Boot + Temporal)
plus die Integrationstests. Dieses Repo wurde aus den vormals getrennten Repositories
`quad`, `kafka-platform` und `agentflow` mit **neuer Historie** zusammengeführt.

## 📂 Struktur

```text
.
├── pom.xml               # Maven-Aggregator (quad-quarkus, kafka-platform, agentflow)
├── README.md             # diese Datei
├── .gitignore            # konsolidiert aus allen Modulen
├── quad-quarkus/         # QUAD – LLM-Agent-Framework (Quarkus) + React-UI
├── kafka-platform/       # Event-getriebene Workflow-Plattform (Kafka + quad-core Worker)
├── agentflow/            # Durable Agent-Workflows (LangGraph4j + Spring AI + Temporal) + Angular-UI
└── integration-tests/    # End-to-End-Tests (eigener Reaktor, eigenes groupId)
```

Root-Ebene trackt dieses Repo bewusst **nur** `README.md`, `pom.xml` und `.gitignore`;
lokale Hilfsdateien (Makefile, `start-*.sh`, `*.md`-Notizen, `.code-workspace`) bleiben
untracked (siehe `.gitignore`).

## 🧩 Module

| Modul | Stack | Inhalt |
|---|---|---|
| `quad-quarkus/` | Quarkus + LangChain4j | Agent-Framework: Agents/Tools/Guardrails/HITL/MCP/Observability, Workflow-Engine (`quad.workflow` v2), React-UI (`quad-ui`) |
| `kafka-platform/` | Spring Boot + Kafka | Orchestrator, Worker, `quad-workflow-worker`, Unified-API-Gateway/Bridge |
| `agentflow/` | Spring Boot + Spring AI + Temporal | Durable, graph-basierte Agent-Workflows (LangGraph4j), deklarative v2-Definitionen, Angular-UI |
| `integration-tests/` | Maven + JUnit | End-to-End-Tests über die Plattform; eigener Reaktor (`de.augmentia.it`) |

## 🚀 Build & Test

Der Root-Aggregator löst `kafka-platform/quad-workflow-worker` → `de.augmentia.quad:quad-core`
**aus dem Reaktor** auf (sonst zieht Maven ein veraltetes Jar aus `~/.m2`):

```bash
# alles (korrekte Reihenfolge, inkl. Tests)
mvn -T1C clean install

# einzelne Reaktoren
cd quad-quarkus    && mvn clean install     # quad-core + Quarkus + UI
cd kafka-platform  && mvn clean install     # Orchestrator/Worker/quad-workflow-worker
cd agentflow       && mvn clean package     # App + UI (quad-core aus ~/.m2)

# Integrationsprojekt (eigener Reaktor)
cd integration-tests && mvn verify
```

> Nach Änderungen an `quad-core` immer `mvn clean package` in den Konsumenten
> (Stale-Jar-Trap: ohne `clean` wird die alte `quad-core` ins Fat-Jar gebündelt).

### Frontends

```bash
cd quad-quarkus/quad-ui      && npm install && npm run build
cd agentflow/agentflow-ui    && npm install && npm run build
```

## 🔗 Backend-Kommunikation & Migrationspfad

| Layer | Quad-Quarkus | Kafka-Platform | Bridge |
|---|---|---|---|
| **Frontend** | React SPA (`quad-ui`) | *(Future Vue/Svelte oder unified)* | Ein SPA, beide via Module Federation/iframe |
| **APIs** | `/api/ui/workflows…` | `/api/v1/workflow-definitions` | `UnifiedApiGateway` proxyt beide |
| **Workflow-Schema** | `{nodes[], edges[]}` | `{dag_structure:{nodes[]}}` | `WorkflowDefinitionBridge` mappt JSON |
| **Monitoring** | PromQL in Quarkus-UI | Spring Actuator `/actuator/prometheus` | ⚠️ kein gemeinsamer Aggregator |

## 🧭 Migrationsphasen

1. **Phase 1 – Paralleler Betrieb** *(aktuell)*: beide Stacks nebeneinander, Bridge erlaubt
   `quad-ui` → Kafka-Workflows.
2. **Phase 2 – Unified Gateway**: Frontend-Traffic über ein Gateway, das das Backend abstrahiert.
3. **Phase 3 – Quarkus deprecaten**: verbleibende Features (Skills, Hooks, MCP) in die
   Kafka-Plattform migrieren, `quad-quarkus` archivieren.

## 📌 Hinweise

- **Neue Historie:** Dieses Repo startet mit einem frischen Initial-Commit. Die alten
  Repositories (`augmentia-de/quad`, `augmentia-de/kafka-platform`, `augmentia-de/agentflow`)
  bleiben als Historie/Backup bestehen.
- **Secrets:** nur `*.example`-Templates sind getrackt; echte `.env`-Dateien sind ignoriert.
