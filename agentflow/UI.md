# AgentFlow UI — Design & Spezifikation

**Status:** Entwurf (2026-10-10) · Ziel: Angular-Webapp im Modul `agentflow-ui`.

Diese Datei beschreibt die geplante Benutzeroberfläche für AgentFlow: Informationsarchitektur,
Screens, Interaktionen, Datenfluss, API-Abbildung, Design-System und Roadmap. Die Umsetzung
erfolgt als eigenständiges Angular-Modul (`agentflow-ui/`), das das Backend (`agentflow-app`)
über REST nutzt.

---

## 1. Ziele & Leitprinzipien

AgentFlow ist **durabel** (Temporal), **graph-basiert** (LangGraph4j) und **quad-komponiert**
(Tools/Skills/Guardrails/Memory/Saga/…). Die UI macht genau diese Fähigkeiten bedienbar:

1. **Observe first** — Runs, Status, Metriken, Provenance/Audit sichtbar machen.
2. **Start in Sekunden** — jeder Engine/Case per Formular startbar, ohne curl.
3. **Editierbar ohne Code** — deklarative `quad.workflow`-v2-Definitionen (Workflow **und**
   Agenten) im Browser validieren und speichern.
4. **Human-in-the-Loop** — pausierte Runs finden, Entscheidungen senden.
5. **Sicherheit & Transparenz** — effektive Settings anzeigen, **niemals** Secrets.
6. **Keine Wartungslast** — eine SPA, ein Build-Artefakt, statisch auslieferbar.

**Nicht-Ziele (v1):** Drag-&-Drop-Graph-Editor, Nutzerverwaltung/Login, Multi-Tenant,
Persistenz der Workflow-Änderungen über Neustart (Runtime-Override statt Datei-Write).

---

## 2. Tech-Stack & Architektur

| Aspekt | Entscheidung | Begründung |
|---|---|---|
| Framework | **Angular 20**, Standalone Components | Vorgabe; Signals + `provideHttpClient` |
| State | **Signals** + kleine Stores (kein NgRx) | geringe Komplexität, reaktiv |
| Routing | `provideRouter` mit Lazy-Loading | schneller Start |
| HTTP | `HttpClient` + typisierte Services | REST gegen `agentflow-app` |
| Styling | SCSS + CSS-Variablen (eigenes Design-System) | keine schweren UI-Deps |
| Build | Angular CLI / `ng build` | statische Assets → vom Backend/Reverse-Proxy auslieferbar |
| Tests | Vitest/Jasmine (CLI-Default) + Services gemockt | Buildbarkeit absichern |

**Deployment-Wege**
- Dev: `ng serve` (Proxy ist in `angular.json` verdrahtet: `proxy.conf.json` → `/agentflow`, `/actuator`, `/examples` gehen an `http://localhost:8095`).
- Prod: `ng build` → `dist/` statisch; entweder eigenes Static-Hosting oder vom Backend
  (`spring.web.resources`) unter `/ui` ausgeliefert.

---

## 3. Informationsarchitektur

```
┌──────────── Sidebar ────────────┐
│ ▣ Dashboard        (/)           │   Übersicht: Health, Metriken, Counts, Quick-Start
│ ▶ Runs             (/runs)       │   starten + Liste + Detail + Signale
│ ⧉ Workflows        (/workflows)  │   deklarative Defs + Editor (v2 JSON) + Validate/Save
│ ✦ Generate         (/generate)   │   Aufgabe → LLM-generierter v2-Workflow (Vorschau/Save/Run)
│ ⚙ Agents           (/agents)     │   Agenten aus allen Defs + Editor
│ ⏸ HITL             (/hitl)       │   pausierte/approval-pflichtige Runs + Entscheidungen
│ ∿ Observability    (/observe)    │   Metriken, Provenance, Audit, Temporal-Link
│ ☰ Examples         (/examples)   │   die fünf Business-Cases
│ ⚙ Settings         (/settings)   │   effektive Konfiguration (read-only, ohne Secrets)
└──────────────────────────────────┘
Topbar: App-Titel · Backend-Status-Badge (Health) · Temporal-UI-Link · Theme-Toggle
```

---

## 4. Screens im Detail

### 4.1 Dashboard `/`
**Zweck:** Lagebild + Einstieg.
- **Statuskarten:** Backend (`/actuator/health`), Temporal-Verbindung (abgeleitet aus Runs-Liste),
  Anzahl Workflow-Definitionen (`/agentflow/workflows`), Anzahl Capabilities (`/agentflow/capabilities`).
- **Metrik-Kacheln:** `agent.llm.calls`, `agent.tool.executions`, `agent.errors`,
  `agent.execution.duration` (aus `/actuator/prometheus`, gefiltert).
- **Quick-Start:** Dropdown „Engine" (quad, quad-rich, authorized, structured, provenance,
  subagents, metrics, capability-search, saga, declarative, examples/*) + Prompt-Feld → startet.
- **Letzte Runs:** aus der Runs-Liste (`/agentflow/runs?limit=10`).
- **Engine-Info:** `/agentflow/info` (graph/durability/quad-reuse).

### 4.2 Runs `/runs`, `/runs/:id`
**Liste:** Tabelle mit `workflowId`, `workflowType`, `status`, `startTime`, `duration`, `taskQueue`.
- Filter: Freitext (Temporal-Query, z. B. `ExecutionStatus = "Running"`), Zeitraum, Typ.
- Aktionen je Zeile: *Detail*; *Terminate* nur bei laufenden Runs (`status = RUNNING`) — beendete
  Runs lassen sich nicht terminieren (Backend antwortet dann `409 Conflict`).
- **Start-Panel** (rechts/oben): Formulare je Engine mit den jeweiligen Parametern
  (input, tools, roles, orderId+amount, op/scope/content, serverUrl/tool/args). Bei der Engine
  **declarative** ist `workflowId` ein **Dropdown der vorhandenen Definitionen**
  (`GET /agentflow/workflows`, Anzeige `Name (id)`) + Task — keine ID-Eingabe nötig.

**Detail `/runs/:id`:**
- Kopfdaten + Status-Badge, History-Länge, Timing (`describe`).
- **Ergebnis** (falls abgeschlossen): Payload-String (JSON pretty-print).
- **Signale:** kontextabhängig „HITL: approve/reject", „Declarative: approve", „Escalate".
- **Timeline:** Rohereignisliste (falls verfügbar) + Deep-Link in die Temporal-UI.

### 4.3 Workflows `/workflows`, `/workflows/:id`
**Liste:** id, name, version, status, Anzahl Nodes, Agent-Refs. **+ Neuer Workflow** (Name eingeben)
→ `POST /agentflow/workflows` legt ein leeres v2-Dokument (nur `end`) an und öffnet es im Detail.
**Detail:**
- **Graph-Ansicht** (v1: verschachtelte Liste/Tabelle nach `dependsOn`-Topologie,
  `type`-Badges `agent|decision|loop|fork|join|hitl|subworkflow|end`,
  `requiresHumanApproval`-Marker).
- **Agenten** des Dokuments (`agents[]`): id, model, tools, systemPrompt, guardrails/hooks.
- **Editor:** JSON-Textarea des v2-Dokuments mit:
  - *Validate* → `POST /agentflow/workflows/validate` (Publish-Compiler + Semantic-Validator).
  - *Save* → `PUT /agentflow/workflows/{id}` (Runtime-Override, sofort lauffähig).
  - Fehlerliste mit JSON-Pointer; Buttons *Formatieren*, *Zurücksetzen*, *Herunterladen*.
- **Run this** → startet `POST /agentflow/runs/declarative?workflowId=…&task=…`.

### 4.4 Generate `/generate`
**Zweck:** aus einer Aufgabe einen lauffähigen v2-Workflow erzeugen (LLM-Dekomposition).
- Formular: Aufgabe (Textarea), Name (optional).
- Aktionen:
  - *Vorschau* → `POST /agentflow/workflows/generate` (`{task,name?}`) → publiziertes v2-Dokument
    (nicht persistiert).
  - *Speichern* → `POST /agentflow/workflows/generate/save` → Runtime-Override + Datei; erscheint
    unter Workflows/Agents.
  - *Ausführen* → `POST /agentflow/workflows/generate/run` → durabler Temporal-Lauf (Ergebnis unter Runs).
- Vorschau: Knoten-/Agent-Zähler, Node-Tabelle (id/type/Titel/dependsOn/agent), Agent-Chips und JSON.

### 4.5 Agents `/agents`
- Flache Liste aller Agenten über alle Workflow-Definitionen (id, name, model, guardrail-Summary,
  Def).
- **Editor:** bearbeitet den `agents[]`-Ausschnitt des zugehörigen Dokuments (Formular:
  id/name/model/temperature/maxTokens/tools/systemPrompt/jsonOutput) und speichert via denselben
  `PUT`-Endpoint (verändert die Definition, nicht den Workflow-Graph).
- **+ Neuer Agent:** Ziel-Definition wählen → neuer Agent (eindeutige id, Defaults) wird dem
  `agents[]` hinzugefügt (erscheint sofort im Agent-Ref-Dropdown des Graph-Editors).
- **Guardrails & Hooks:** Multi-Selects *Guardrails (Input/Output)* (`pii`, `sql-injection`,
  `prompt-injection`) → `guardrailsInput`/`guardrailsOutput`; *PII-Masken* → Hook `pii:…`;
  Checkbox *Audit-Trail* → Hook `audit`. Werte aus `GET /agentflow/quad/capabilities`.
- *Test-Run*: startet einen quad-Run mit dem `systemPrompt` als Prompt (Schnelltest).

### 4.6 HITL `/hitl`
- Filtert Runs-Liste auf „wartend": `ExecutionStatus = "Running"` **und** Workflow-Typ in
  {`HitlWorkflow`, `DeclarativeAgentWorkflow`, `AuthorizedAgentWorkflow`, `SupportTriageWorkflow`,
  `ContractReviewWorkflow`}.
- Karte je Run: Typ, Startzeit, wartet auf was; Aktionen *Approve* / *Reject* / *Escalate*.
- Optional: Aging-Indikator (wie lange wartend).

### 4.7 Observability `/observe`
- **Metriken:** aus `/actuator/prometheus` gefilterte `agent_*`-Familien; `agent.llm.calls`,
  `agent.tool.executions`, `agent.errors`, `agent.execution.duration.seconds_*`, Token-Summaries.
- **Provenance:** Button „Provenance-Run" → `POST /agentflow/runs/provenance`, Ergebnis als Tabelle
  (Tool, Quelle, Zeit).
- **Audit/Logs:** geführter Run (`/runs/provenance`, `/runs/metrics`) + Roh-Prometheus-Text
  als aufklappbarer Block; Deep-Link Temporal-UI für Event-Historie.
- **Hinweis:** Es gibt (noch) kein Log-File-API; Logs = Metriken + Run-Historie + Audit-Hook-Ausgabe.

### 4.8 Examples `/examples`
- Karten aus `GET /examples` (id, title, endpoint, capabilities).
- Pro Case ein startbares Formular (liest den `endpoint`-Pfad und bietet die passenden Felder).

### 4.9 Settings `/settings`
- **Read-only** effektive Konfiguration (`GET /agentflow/settings`), gruppiert:
  - *LLM*: model, base-url, `hasApiKey` (boolesch — **kein** Key), `headersConfigured` + Header-**Namen**.
  - *Durability*: temporal target/namespace, taskQueue.
  - *Capability-Search*: index (hnsw/pgvector), table, autoload, cache.
  - *Workflows*: location.
  - *Checkpoint*: path.
  - *Endpoints*: metrics, temporal-ui.
- Änderungen bewusst **nur** über Env/Neustart (kein Hot-Config v1) — UI zeigt, wo gesetzt wird.
- **Sicherheitsregel:** Secrets werden nie serialisiert oder angezeigt.

---

## 5. API-Abbildung (Backend-Endpoints)

### Bestehend
| Methode | Pfad | Zweck |
|---|---|---|
| POST | `/agentflow/runs` | LangGraph-Workflow |
| POST | `/agentflow/runs/quad` | quad `Agents.simple()` |
| POST | `/agentflow/runs/quad-rich` | Tools/Skills/Guardrails/PII/Audit |
| POST | `/agentflow/runs/authorized` | RBAC |
| POST | `/agentflow/runs/hitl` + `/hitl/{id}/decide` | HITL |
| POST | `/agentflow/runs/saga` | Saga |
| POST | `/agentflow/runs/quad-saga` | quad Session-Saga |
| POST | `/agentflow/runs/structured` | Structured Output |
| POST | `/agentflow/runs/provenance` | Provenance |
| POST | `/agentflow/runs/memory?op=remember\|recall` | Memory |
| POST | `/agentflow/runs/mcp?op=list\|run` | MCP |
| POST | `/agentflow/runs/subagents` | Sub-Agenten |
| POST | `/agentflow/runs/metrics` | Metriken/OTel |
| POST | `/agentflow/runs/capability-search` | Capability/RAG |
| POST | `/agentflow/runs/declarative` + `/{id}/approve` | deklarativ v2 |
| GET | `/agentflow/info`, `/quad/capabilities`, `/security/roles` | Info/Config |
| GET | `/agentflow/capabilities`, `/capabilities/search?q=` | Capability-Katalog |
| GET | `/agentflow/workflows`, `/workflows/{id}` | deklarative Defs |
| GET | `/examples` | Business-Cases |
| GET | `/actuator/health`, `/metrics`, `/prometheus` | Observability |

### Neu (für die UI ergänzt)
| Methode | Pfad | Zweck |
|---|---|---|
| GET | `/agentflow/runs?query=&limit=` | Run-Liste (Temporal-Visibility) |
| GET | `/agentflow/runs/{id}` | Run-Detail (Status + ggf. Ergebnis) |
| POST | `/agentflow/runs/{id}/terminate` | Admin-Abbruch |
| POST | `/agentflow/workflows/validate` | v2-Dokument prüfen (Compiler+Semantik) |
| PUT | `/agentflow/workflows/{id}` | v2-Dokument speichern (Runtime-Override) |
| GET | `/agentflow/settings` | effektive Config ohne Secrets |
| GET | `/agentflow/runs/stream?query=` | **SSE**: Run-Liste live |
| GET | `/agentflow/runs/{id}/stream` | **SSE**: Status **+ History-Events** bis terminal |
| GET | `/agentflow/runs/{id}/events?max=` | History-Events (Log) als JSON |
| POST | `/agentflow/testbench/run` | Einzel-Agent-Trace (Tokens/Tools/Kosten/Provenance) |
| GET | `/agentflow/workflows/{id}/versions` | Version-Snapshots |
| GET | `/agentflow/workflows/{id}/versions/{v}` | Version laden |
| POST | `/agentflow/workflows/generate` | LLM: Aufgabe → v2-Dokument (Vorschau) |
| POST | `/agentflow/workflows/generate/save` | generieren + speichern (Override + Datei) |
| POST | `/agentflow/workflows/generate/run` | generieren + durabel ausführen (Temporal) |
| POST | `/agentflow/workflows` | neuen, leeren Workflow (nur `end`) anlegen |

Optionale Header (nur wenn im Backend aktiviert): `X-API-Key`/`Authorization: Bearer`
(API-Key-Gate), `X-Tenant-Id` (Capability-Scoping).

---

## 6. Datenfluss & State

```
ApiService (HttpClient)
   ├── HealthService      → /actuator/health            (Poll 10s)
   ├── RunsService        → /agentflow/runs(/…)         (Poll 5s)
   ├── WorkflowService    → /agentflow/workflows(/…), validate, save
   ├── AgentService       → abgeleitet aus Workflows (+ Test-Run)
   ├── CapabilityService  → /agentflow/capabilities(/search)
   ├── MetricsService     → /actuator/prometheus        (Poll 15s)
   ├── ExampleService     → /examples
   └── SettingsService    → /agentflow/settings

Stores (Signals):
   RunsStore   { runs, filter, selected, loading, error }
   WorkflowStore { definitions, draft, validationErrors, dirty }
   ToastStore  { messages }
```
- **Polling** statt SSE/WebSocket (Temporal liefert keine Push-API an den Client).
- Fehler werden zentral als Toast gezeigt; HTTP-Status sichtbar.

---

## 7. Design-System

- **Farben:** dunkles Theme als Default, helles Theme via CSS-Variablen umschaltbar.
  Akzente: Indigo (primär), Amber (warn/waiting), Grün (ok), Rot (Fehler).
- **Typo:** System-Stack; Monospace für IDs/JSON.
- **Komponenten:** `AppShell`, `StatCard`, `DataTable`, `StatusBadge`, `JsonEditor`,
  `FormField`, `Toast`, `Tabs`, `EmptyState`, `Spinner`.
- **Status-Farben:** RUNNING=blau, COMPLETED=grün, FAILED/TIMED_OUT=rot, CANCELED/TERMINATED=grau,
  WAITING/approval=amber.
- **Barrierefreiheit:** Fokus-Ringe, `aria-label`, ausreichende Kontraste, Tastaturbedienung.
- **Sprache:** Deutsch (Labels/Fehlermeldungen), Code/IDs englisch.

---

## 8. Sicherheit

- Keine Secrets im UI/API-Response; `/settings` liefert nur `hasApiKey`.
- Admin-Aktionen (Terminate) mit Bestätigungsdialog.
- CSP-freundliche, statische Assets; keine externen CDNs.
- CORS: im Dev über Angular-Proxy umgangen; in Prod empfohlen, UI vom selben Origin auszuliefern.

---

## 9. Umgesetzt (v2)

1. **Persistenz & Versionierung**: `WorkflowDefinitionStore` schreibt publizierte Defs nach
   `agentflow.workflows.state-dir` und legt vor jeder Überschreibung einen Snapshot unter
   `versions/<id>/<timestamp>.json` ab (git-fähig). UI-Tab „Versionen“ (laden/ansehen).
2. **Visueller Graph-Editor**: `af-graph-editor` — Layered-Layout, Drag (Position je Workflow in
   localStorage), **Dependencies setzen** per Checkbox-Liste und **Click-to-Connect** (🔗 Quelle→Ziel),
   Umbenennen/Löschen mit **Referenz-Integrität** (aktualisiert `dependsOn`/Branches/`bodySteps`/`defaultTarget`),
   `+ Node`/`+ verbunden`, Ziele als Dropdowns. Änderungen werden schema-konform ins JSON übernommen.
3. **SSE komplett**: `/agentflow/runs/stream` (Liste) und `/agentflow/runs/{id}/stream`
   (Status **+ inkrementelle History-Events**); zusätzlich REST `GET /agentflow/runs/{id}/events`.
   UI zeigt ein Live-Log (Event-ID/Typ/Detail/Zeit) und nutzt `EventSource` mit REST-Fallback.
4. **Alle Knotentypen im Runner**: `DeclarativePlanner` flacht (Sub-Workflows inline, Alias auf
   `end`) + `DeclarativeAgentWorkflow` interpretiert `agent/decision/loop/fork/join/hitl/end`
   (dependency-getrieben, Branch-Pruning, Loop-Exit-Conditions, per-Node-HITL-Signal `decide`).
5. **Auth: OIDC (deaktiv) + API-Key/Tenant (opt-in)**: `WebSecurityConfig` liefert permissiv
   (Default, `agentflow.security.oidc.enabled=false`); bei Aktivierung wird ein
   **JWT-Resource-Server** (`issuer-uri`/`jwk-set-uri`) geschaltet. Separat `ApiKeyFilter`
   (`agentflow.security.enabled`) und `TenantFilter` (`X-Tenant-Id`-Scoping).
6. **Agent-Testbench**: `/agentflow/testbench/run` liefert Antwort, Input/Output-Tokens,
   Tool-Calls, Dauer, Kostenschätzung und Tool-Provenance; UI-Seite „Testbench“.
7. **Dynamische Workflow-Generierung**: `/agentflow/workflows/generate[/save|/run]` — ein LLM
   („Workflow-Architect“, quad-core-Pfad) zerlegt eine Aufgabe in einen v2-Workflow (Agent-Schritte
   + genau ein `end`), publiziert über denselben Compiler/Validator; UI-Seite „Generate“
   (Vorschau/Speichern/Ausführen).
8. **Agenten/Workflows anlegen + Guardrails/PII/Audit**: `POST /agentflow/workflows` (leerer
   Workflow), „+ Neuer Agent“ auf `/agents`. Deklarative Guardrails (`guardrailsInput`/
   `guardrailsOutput`), PII-Maskierung und Audit (`hooks`) werden im Runner über quad-core's
   `GuardrailPlugin` (Hook-Pipeline, `beforeModelCall`/`afterModelCall`), `PiiAnonymizerHook` und
   `AuditTrailHook` durchgesetzt; Namen über `GET /agentflow/quad/capabilities`.

### Roadmap (weiter)
- OIDC aktivieren + Keycloak-Realm (Rollen → Tenant-Mapping); `agentflow.security.oidc.enabled=true`.
- Persistenz-Trigger „git commit“ bei Save (Hook).
- Graph-Editor: Undo/Redo, Multi-Select, Kanten-Löschen per Klick auf die Linie.
- `decision`-Angleiche an quad/kafka (Regex/Expression) — Vergleich siehe `DECISION_NODE_COMPARISON.md`,
  bewusst **noch nicht** umgesetzt.

---

## 10. Umsetzungs-Checkliste (v1)

- [x] App-Shell + Routing + Design-System
- [x] Dashboard (Health, Metriken, Counts, Quick-Start)
- [x] Runs (Liste, Detail, Start-Formulare, Signale/Terminate)
- [x] Workflows (Liste, Detail, JSON-Editor mit Validate/Save)
- [x] Agents (Liste, Editor, Test-Run)
- [x] HITL (wartende Runs + Entscheidungen)
- [x] Observability (Metriken, Provenance, Temporal-Link)
- [x] Examples (Karten + Formulare)
- [x] Settings (read-only, ohne Secrets)
- [x] Visueller Graph-Editor (Drag/Properties, alle Node-Typen)
- [x] Persistenz + Versionen (Datei + Snapshot, UI-Tab)
- [x] SSE-Live-Updates (Liste + Einzel-Run **mit History-Events**)
- [x] Runner für alle Knotentypen (decision/loop/fork/join/subworkflow/hitl)
- [x] Auth (OIDC **deaktiv** + API-Key/Tenant opt-in) + Testbench-Seite
- [x] Generate (LLM-Aufgabe → v2-Workflow: Vorschau/Speichern/Ausführen)
- [x] Graph-Editor: Dependencies setzen/verbinden, Add/Rename/Delete
- [x] `decision`-Vergleich quad/kafka/agentflow (`DECISION_NODE_COMPARISON.md`)
- [x] Proxy + Build + README
