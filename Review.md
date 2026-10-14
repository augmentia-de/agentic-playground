# Review.md — Architektur- und Code-Review der Agentic Platform

| | |
|---|---|
| **Repository** | `agentic-platform` (Monorepo: quad-quarkus, kafka-platform, agentflow, integration-tests) |
| **Revision** | Branch `main`, HEAD `34c30c2` („Translations") |
| **Datum** | 2026-10-14 |
| **Rollen** | Architekt & Reviewer |
| **Methode** | Manuelle Quellcode-Analyse (`read`/`grep`), Modul-Deept-Dives, Codebase-Memory-Graph (nur `quad-quarkus` indiziert), Git-Inventar. Kein Build/Test ausgeführt. |
| **Umfang** | ~98.000 Java-Zeilen, 947 Java-Dateien, 24 Maven-Module, 2 Frontends (React + Angular), 49 getrackte Markdown-Dateien |

> **Einordnung/Evidenz.** Dies ist ein Review auf **Verify-Niveau (Tier 2)**: Aussagen sind mit `Datei:Zeile` belegt oder per `grep`/Inventar verifiziert. Es ist **kein** erschöpfender Audit; „kein Aufrufer gefunden" bedeutet „im geprüften Modulsatz kein Produktionsaufrufer", nicht „beweisbar tot". `agentflow` und `integration-tests` waren nicht im Graph indexiert, dort wurde ausschließlich direkt im Code geprüft.

---

## 1. Executive Summary

Das Monorepo ist **inhaltlich ambitioniert und gut erzählt**: eine gemeinsame Agenten-Basis (`quad-core`), drei Orchestrierungsstufen (synchron → eventgetrieben → durable), konsistente README-Struktur. Die größten **Stärken** sind die sauberen Ports/Adapter im Workflow-Engine, die Selbst-Dokumentation der Lücken (`Luecken.md`, `Optional_Improvements.md`) und das `@Disabled`-Befund-Muster in den Integrationstests.

Die größten **Schwächen** sind vier Muster, die sich durch das ganze Repo ziehen:

1. **„Shared basis" stimmt nur auf dem Papier.** Der Agent-Kern ist geteilt, aber Orchestrierung und Code-Generierung wurden je Stufe kopiert und sind **divergiert** — insbesondere `WorkflowGeneratorService` in `agentflow` ist eine unvollständige Kopie des `quad-quarkus`-Originals (Join-Logik fehlt).
2. **Dokumentationsdrift.** Sechs Top-Level-Status-/Spec-Dokumente in `kafka-platform` nennen Pakete (`com.agentplatform.*`, `com.platform.*`), Module und Testzahlen, die es **nicht gibt**; `spec.md` ist ein kaputtes KI-Artefakt.
3. **Sicherheits-/Korrektheitsfehler an den Systemgrenzen.** Pfad-Confine-Bypass und SSRF im `PrefixSourceResolver`, invertierte Vault-Health-Prüfung, ein JWT-Filter, der 500er zu 401 maskiert, ein unauthentifizierter Slack-HITL-Callback, GDPR-Endpunkte als Attrappen.
4. **Toter/„implementierter" Code.** Vault, Schema-Registry, Slack/PagerDuty, PrefixCache und der Mail-Responder sind laut Doku „fertig", aber nicht verdrahtet bzw. Attrappen; dazu ~9 ungenutzte Klassen in `quad-core` und ein komplettes, nicht kompiliertes `src/test/tmp`-Verzeichnis.

### Top-10-Prioritäten (Kurzfassung)

| # | Schwere | Befund | Fundstelle |
|---|---------|--------|-----------|
| 1 | **Kritisch** | E-Mail-getriggerte Workflows können nie starten (Producer-/Consumer-Eventform inkompatibel) | `kafka-platform` `EmailInputTransformer` vs. `DagTopicListener`/`WorkflowEngine.java:112` |
| 2 | **Kritisch** | Worker bestätigt Kafka-Records vor bestätigtem Publish → stiller Ergebnisverlust | `WorkerResultPublisher.java:36-41`, `TaskExecutor.java:140-183`, `WorkflowTaskListener.java:107-132` |
| 3 | **Hoch** | Willkürliches Host-Datei-Lesen + SSRF in `PrefixSourceResolver` | `PrefixSourceResolver.java:94-121` |
| 4 | **Hoch** | Session-ID ungeprüft bis in Docker-Mount/Pfade | `WorkspaceResolver.java:46-48`, `SandboxPoolClient.java:262-265` |
| 5 | **Hoch** | Symlink-Confine durch lexikalischen Fallback ausgehebelt | `WorkspaceResolver.java:161-189` |
| 6 | **Hoch** | `agentflow`-Generator: Join-Logik fehlt → parallele Forks werden nie gemerged | `agentflow/.../WorkflowGeneratorService.java` vs. `quad-quarkus/...:465,548` |
| 7 | **Hoch** | Vault `isConnected()` invertiert (`initialized && sealed`) | `SecretResolver.java:191-192` |
| 8 | **Hoch** | JWT-Filter verwandelt Downstream-Ausnahmen in 401 | `JwtAuthenticationFilter.java:91-97` |
| 9 | **Mittel** | Import/Export ohne semantische Validierung (v2-Pipeline tot) | `WorkflowTransferService.java:132` |
| 10 | **Mittel** | Dokumentations- und Testzahl-Drift; kein CI für `quad-quarkus`/`agentflow` | `kafka-platform/*.md`, `kafka-platform/.github/workflows/ci.yml` |

---

## 2. Repository-Überblick (Ist-Stand)

```
agentic-platform/
├── pom.xml                  # Aggregator, BEWUSST ohne Parent (pom.xml:3-19)
├── quad-quarkus/            # Stage 1: Quarkus + LangChain4j + React-UI   (605 Java-Dateien)
│   ├── quad-core            #   gemeinsame Agenten-Basis (238 Dateien, 16.5k LOC)
│   ├── quad-workflow-core   #   typisiertes Workflow-Modell + Transfer/Import
│   ├── quad-workflow-engine #   Map-basierte Engine + Node-Executors
│   ├── quad-tool-builtin / quad-tool-mcp / quad-sandbox-docker
│   ├── quad-contract        #   API-DTOs
│   └── quad-quarkus         #   REST/SSE-App + React-Frontend
├── kafka-platform/          # Stage 2: Spring Boot + Kafka                  (199 Java-Dateien)
│   ├── agent-common         #   Domain, DAG/Routing, Events, Topics + SPI
│   ├── agent-orchestrator   #   Engine, Checkpoints, HITL, DLQ, Vault/JWT
│   ├── agent-worker         #   Legacy-Mock (konsumiert keine echten Topics)
│   └── quad-workflow-worker #   echter LLM-Worker (quad-core, Spring AI)
├── agentflow/               # Stage 3: Temporal + Spring AI                 (129 Java-Dateien)
│   ├── agentflow-core       #   LangGraph4j-Graph/State
│   ├── agentflow-app        #   Temporal-Workflows/Activities + REST
│   └── agentflow-ui         #   Angular-Frontend
└── integration-tests/       # L0–L3 + syscapability (104 @Test) — eigener Reaktor
```

**Git-Inventar:** 1.378 getrackte Dateien, 49 `.md`, keine `node_modules/`, keine `target/` (gut). Am Root sind bewusst nur `README.md`, `pom.xml`, `.gitignore` getrackt.

---

## 3. Architektur-Review

### 3.1 Was architektonisch gut ist

- **Klares Stufenmodell** mit einer gemeinsamen Basis; die README-Tabelle „Was ändert sich pro Stufe" ist vorbildlich.
- **Ports/Adapter in der Workflow-Engine** (`workflow/adapter/*`: `WorkflowDefinitionAdapter`, `StepCheckpointAdapter`, `AgentInvocationAdapter`, `RunEventAdapter`) isolieren die Engine sauber von Quarkus/CDI; `WorkflowEngineProducer` ist ein einziger Wiring-Punkt.
- **Sicherheits-Defaults im Sandbox-Client**: `--network none`, `--rm`, Memory-Cap, Label-basierter Stub-Fallback statt Hängen (`SandboxPoolClient.java:80-118`).
- **Idempotenz-/Replay-Bewusstsein in Stage 3**: Declarative-Workflow trennt deterministische Planung von nichtdeterministischen Activities.
- **Fail-closed** an mehreren Stellen (z. B. `ApiKeyFilter` bei aktivierter Security, Upcaster wirft bei Unbekanntem).

### 3.2 Architekturprobleme

**A1 — „Shared basis" ist teilweise eine Fiktion (Drift statt Wiederverwendung).**
Der Agent-Kern ist geteilt, aber **Orchestrierung und Code-Generierung sind je Stufe kopiert**:

- `agentflow/agentflow-app/src/main/java/de/augmentia/agentflow/app/workflow/generate/WorkflowGeneratorService.java` ist eine near-verbatim-Kopie von `quad-quarkus/quad-quarkus/src/main/java/de/augmentia/quad/quarkus/workflow/WorkflowGeneratorService.java` (identische Methodennamen), aber **unvollständig**: 17 vs. 29 `private`-Methoden und **keine** Join-Logik (`grep 'join'` → 0 Treffer vs. 11 im Original, u. a. `:465 addJoinNodes(...)`, `:548`).
- Es existieren **drei überlappende Workflow-Modelle** (`quad-workflow-core` typisiert, `quad-workflow-engine` Map-basiert, `kafka-platform/agent-common` DAG). `NestedWorkflowNodeExecutor` überbrückt sie durch Re-Fetch aus `SharedState` (`:51-66`) statt über einen gemeinsamen Typ.
- **Zwei UIs** (React `quad-ui`, Angular `agentflow-ui`) mit getrennten, duplizierten API-Clients.

> Konsequenz: Ein Feature, das in einer Stufe entsteht (Join-Nodes, Prefix-Cache, HITL-Gating), **divergiert still** in den anderen.

**A2 — God-Services / fehlende Schichtung.**
`WorkflowGeneratorService` (776 Zeilen: LLM-Call + JSON-Reparatur + DAG-Bau + Persistenz), `AgentController` (771), `SystemController` (390), im Kern `Agent.java` (577, 15 injizierte Felder `:39-53`) und `AgentRuntime.java` (641). Verantwortlichkeiten sind nicht geschnitten; Testbarkeit und Verständlichkeit leiden.

**A3 — Root-Aggregator bewusst ohne Parent.**
`pom.xml:3-19` erklärt das bewusst (unterschiedliche Eltern/BOMs). Das ist nachvollziehbar, kostet aber **zentrale Dependency-/Version-/Plugin-Verwaltung**; Versionen und Plugin-Konfigurationen werden über drei Reaktoren hinweg dupliziert gepflegt.

**A4 — Kein monorepo-weites CI.**
Die einzige CI ist `kafka-platform/.github/workflows/ci.yml` und läuft nur für `kafka-platform`. **`quad-quarkus` und `agentflow` haben keine CI**, der Root-Reaktor wird nie integriert getestet. Die im `README.md` genannten Testzahlen sind nirgends CI-verifiziert.

**A5 — Doppelte Konzepte / Namenskollisionen.**
`AuditStore` (×2, JDBC vs. in-memory), `HookRegistry` (×2), `OrchestratorAgent` (×2), `Capability` (×2 in `quad-core`), `WorkflowGeneratorService` (×2). Gleiche Simple-Names für verschiedene Dinge erschweren Navigation und Import-Korrektheit.

**A6 — UI/Config-Leak in die Engine.**
`RunEventAdapter` reicht `SharedState.incMetrics()` durch (`RunEventAdapter.java:74-76`); die Engine kennt damit eine UI-Metrik. `WorkflowEngine` baut Event-JSON per String-Konkatenation und legt so das Serialisierungsformat offen (`:57-58,110,127`).

**A7 — Fachliche Attrappen im Produktionspfad.**
`SystemController.getMetrics/getErrorMetrics` liefern **erfundene Zahlen** (`:159-166`); `AutomationService.lastAssistantText` gibt einen Platzhalter `"[session-… transcript]"` zurück (`:174`) und persistiert ihn als Ergebnis; Auto-Scaling schreibt nur einen Redis-Wert, skaliert aber nichts.

---

## 4. Ungenutzter / toter Code

**Regel:** „kein Produktionsaufrufer im geprüften Modulsatz gefunden" (per `grep`/Graph). Einzelne Treffer können Test-/Reflexionsnutzung sein.

### 4.1 `quad-core`

| Symbol | Fundort |
|--------|---------|
| `GuardrailException` | `guardrails/…` |
| `QuadEventStore` | `events/…` |
| `SessionLockService` (zudem funktionslos: leerer `LockEntry.refresh`, `heartbeat` No-op `:40,46`) | `session/SessionLockService.java` |
| `AgentMemory`, `MultiSystemMessageChatMemory` | `session/memory/…` |
| `SummarizingConversationManager`, `SagaAwareConversationManager` (letzterer ein No-op `:11-15`) | `session/memory/…` |
| `StateDiffMemoryBridge` (`@ApplicationScoped`, unsynchronisierte `ArrayList`, nie geleert `:9-11`) | `agent/runtime/…` |
| `ToolSetChangeNotifier` | `tool/…` |

Nur test-verdrahtet: `EventQuery`, `InspectInputsPrefill`, `TextOnlyGuard`, `StructuredOutputValidator`, `TokenRecovery`, `QuadConfig`, `RunSnapshot`, `SlidingWindowConversationManager`, `PredictStrategy`, `ReflexionStrategy`, `TemplateStrategy`, `TimerChannel`.

### 4.2 `quad-quarkus`

| Befund | Fundort |
|--------|---------|
| **v2-Transfer-/Publish-Pipeline komplett toter Produktionscode** (`QuadWorkflowPublishCompiler`, `QuadWorkflowUpcaster`, `QuadWorkflowSemanticValidator`, `QuadWorkflowHitlNormalizer`) — nur Tests/Docs referenzieren sie | Import in `WorkflowTransferService.java:6,132` nutzt nur `QuadWorkflowSchemaValidator` |
| Verwaiste, **nicht kompilierte** E2E-Tests (kein `build-helper`, keine `test/tmp`-Referenz in POMs) | `quad-quarkus/src/test/tmp/` (5 Dateien, u. a. `E2E_FullFlowIntegrationTest.java` 448 Z.) |
| Datei-/Klassenname-Mismatch (`class QuadWorkflowSchemaValidatorTest` in `NooaWorkflowSchemaValidatorTest.java`) | `quad-workflow-core/src/test/…` |
| Tote Methoden | `AgentController.matches*` `:736-769`; `McpManagerService.bridgeToSystemProperties` `:241-249`; `WorkflowEngine.onMessage/hasPendingHandler` `:189-199`; `AgentStepExporter.reset` `:98-102`; `AutomationService.scheduledPrompt` `:176-179` |
| Totes Feld (und Leak) | `SandboxPoolClient.executor` `:48,69` |

### 4.3 `kafka-platform` — „implementiert", aber unerreichbar

| Komponente | Fundort | Doku-Behauptung |
|-----------|---------|-----------------|
| `SlackNotifier` | `bridge/alerting/…` | `TODO.md:63`, `SPEC2 §10.2` |
| `PagerDutyAlertSender` | `bridge/alerting/…` | `TODO.md:64` |
| `SchemaRegistryClient` (zudem `registerAll()` nur Log-No-op `:36-38`) | `orchestrator/schema/…` | `TODO.md:57-58` |
| `VaultConfig` / `SecretResolver` | `orchestrator/config`, `bridge/vault` | `TODO.md:61-62` |
| `PrefixCache` | `agent-worker/…` | `TODO.md:87` |
| `MailResponseHandler` (verdrahtet, aber **inert**: `lookupInitiatorEmail` liefert `null` `:161-165` → nie eine Mail) | `bridge/mail/…` | `TODO.md:47` |

### 4.4 `agentflow`

Mehrere parallele `*WorkflowImpl`-/Stub-Klassen (`QuadSagaWorkflow`, `SagaWorkflowImpl`, `RichAgentWorkflowImpl`, `HitlWorkflowImpl`, `AuthorizedAgentWorkflowImpl` …) sind Dead-Code-**Kandidaten**. Temporal-Registrierung pro Klasse wurde nicht durchgängig geprüft — als Audit-Aufgabe markiert, nicht als bestätigt tot.

---

## 5. Offensichtliche Fehler / Bugs

Nach Schwere geordnet. Alle mit `Datei:Zeile`.

### 5.1 Kritisch

**B1 — E-Mail-Workflows starten nie (Eventform inkompatibel).**
`EmailInputTransformer` publiziert `ControlEvents.EmailIntakeEvent` (`workflowId, triggerKey, emailBody, initiatorEmail, receivedAt`) auf `TopicNames.INTAKE` (`EmailInputTransformer.java:58-59`; Record `ControlEvents.java:59-66`). Der Orchestrator liest dasselbe Topic als `WorkflowStartEvent` (`DagTopicListener.java:36`). Keine Feldnamen überschneiden sich → `workflowExecutionId == null` → `UUID.fromString(...)` wirft (`WorkflowEngine.java:112`) → DLQ. Feature laut `NEW.md`/`TODO.md:45-48` „fertig", faktisch tot.

**B2 — Kafka-Record vor bestätigtem Publish bestätigt (stiller Ergebnisverlust).**
`WorkflowResultPublisher.send` (`:36-41`) und `WorkflowDlqPublisher.send` (`:46`) nutzen asynchrones `producer.send(...)` **ohne** `.get()`/Flush. `TaskExecutor.safeExecute` liefert danach sofort `true` (`:140-183`), `WorkflowTaskListener` ackt (`:107-132`). Scheitert das Produce danach, ist der Record bereits bestätigt → verloren, kein Redelivery, kein Shutdown-Flush.

### 5.2 Hoch

**B3 — `PrefixSourceResolver`: willkürliches Host-Datei-Lesen + SSRF.**
`readFile` behandelt jeden mit `/` beginnenden Pfad als absoluten Hostpfad (`:108-121`): `prefixSource: {"file": "/etc/passwd"}` liest beliebige Dateien in den Prompt. `fetchUrl` (`:94-106`) lädt jede `http(s)`-URL inkl. Redirects (kein Allow-List, kein Metadata-IP-Block) → SSRF.

**B4 — Session-ID ungeprüft bis zum Docker-Mount.**
`sessionDir(id) = base.resolve(id)` (`WorkspaceResolver.java:46-48`); die statische `resolve(id)` ist reine Konkatenation (`:36-38`), keine Normalisierung/Containment. Absolute IDs vs. `..`/`a/../../..` entkommen der Basis (Path.resolve(eines absoluten Pfads) ersetzt die Basis). `SandboxPoolClient.workspaceDir` reicht das direkt in einen **rw**-Bind-Mount (`:262-265`, `:105-106`).

**B5 — Symlink-Schutz durch lexikalischen Fallback ausgehebelt.**
`WorkspaceResolver.symlinkSafe` (`:161-172`) gibt das Ziel zurück, wenn der reale Pfad entkommt, **aber** der lexikalische Pfad innen liegt (`if (target.startsWith(realRoot)) return target;`). Ein Symlink im Workspace, der nach außen zeigt, passiert. Gleiches Muster in `symlinkSafeAgainstGrant` (`:174-189`).

**B6 — `agentflow`-Generator verliert Join-Nodes.** Siehe A1. Fork → Merge ohne Merge-Knoten; Downstream-Dependencies auf mehrere Zweige können nicht auflösen.

**B7 — `SecretResolver.isConnected()` invertiert.** `return response.path("initialized") && response.path("sealed")` (`:191-192`) → nur wahr, wenn Vault **versiegelt** ist. Korrekt: `initialized && !sealed`.

**B8 — `@Value`-Felder im Konstruktor.** `SecretResolver` baut den `WebClient` im Konstruktor aus `@Value`-Feldern (`:58-78`, noch `null`) → falsche `baseUrl`, `X-Vault-Token: null`. Gleiches in `SchemaRegistryClient` (`:33`).

**B9 — `WorkflowEngine.onResult` parst UUID vor Validierung.** `UUID.fromString(event.taskId())` (`:139`) vor dem Blank-Check; der Startup-Selfcheck erzeugt bereits ein synthetisches Event → DLQ-Rauschen bei jedem Boot. `onIntake` gleich (`:112`).

**B10 — JWT-Filter maskiert 500er als 401.** `chain.doFilter(...)` steht **innerhalb** des `try`, dessen `catch (Exception)` 401 schreibt (`JwtAuthenticationFilter.java:91-97`). Jede Downstream-Ausnahme wird als „Invalid token" gemeldet; zudem ist der Filter nicht an `app.security.enabled` gekoppelt.

**B11 — `agentflow`-Plan ohne `maxOutputTokens`.** `max-output-tokens: 0` (`application.yml:50`, Default `:0` in `:110`); `chatParametersJson` lässt das Feld dann weg (`:183-184`) → Provider-Default kappt lange Pläne → `parseSteps` scheitert. Das Quad-Original defaultet bewusst groß (`:249-254`).

**B12 — Dynamic-Header/Tenant auf 3 von 4 Activities wirkungslos.** Nur `runAgentGraph` setzt `RequestContext` (`AgentFlowActivitiesImpl.java:35,59`); `runQuadAgent`/`runRichAgent`/`runAuthorizedAgent` (`:78,84,95`) nicht → per-Request-Header/Tenant still ignoriert (und potenzielles ThreadLocal-Leck).

**B13 — GDPR-Endpunkte als Erfolgs-Attrappen.** `deletePersonalData`/`anonymizePersonalData` geben Erfolg zurück, löschen aber nichts (`GdprController.java:87-113,121-136`; explizites `// TODO` `:105`). Bei „GDPR-Export/Delete/Anonymize" ist das ein **Compliance**-Risiko.

**B14 — Unauthentifizierter Slack-HITL-Callback.** `HitlPortalController.slackCallback` (`:240-282`) prüft kein Slack-Signing-Secret → jeder mit Netzzugang kann HITL-Tickets freigeben/überspringen.

### 5.3 Mittel

| # | Befund | Fundort |
|---|--------|---------|
| B15 | Token-Cache nur nach `audience` gekeyt (ignoriert `subjectToken`/`scopes`) → **falscher Token** bei gleicher Audience | `TokenExchangeClient.java:71` |
| B16 | Session-Snapshot verliert `MemorySnapshot.summary` bei `toState()` | `SessionStateSnapshot.java:103-106` |
| B17 | Guardrail-Aktionen `THROW`/`ESCALATE` werden ignoriert (alles → `Cancel`) | `GuardrailPlugin.java:99-104` |
| B18 | `PiiAnonymizerHook` mutiert die Live-Liste in-place | `PiiAnonymizerHook.java:64` |
| B19 | PII-Regex case-sensitiv → Kleinbuchstaben-E-Mails passieren ungefiltert | `AgentBuilder.java:453-456` |
| B20 | OTel-Tracer ignoriert `toolName` (`addEvent`) | `OTelAgentTracer.java:74-79` |
| B21 | Referenzvergleich statt Wertgleichheit (`toKeep == nonSystemMsgs`, `subList`) → toter Zweig, falscher Pfad | `SummarizingSlidingWindowConversationManager.java:70` |
| B22 | `HookRegistry.wrap` verschluckt Exceptions; `failurePolicy` nicht `volatile` | `HookRegistry.java:163-172` |
| B23 | `removeRun` nur im Erfolgspfad → unbeschränktes Leck in `runOutputs` bei Fehlern | `WorkflowEngine.java:98` |
| B24 | `completeDeferred` schreibt bei nicht registriertem Run still nichts, Aufrufer meldet trotzdem Erfolg | `WorkflowSupport.java:169-176` |
| B25 | JSON-Events per String-Konkatenation (nicht escapt) → invalides JSON/Injection | `WorkflowEngine.java:57-58,110,127` |
| B26 | `loadAll*` bricht bei **einer** defekten Zeile die gesamte Ladung ab | `PersistenceStore.java:94-192` |
| B27 | Arrays als CSV gespeichert → Werte mit Komma gehen verloren | `PersistenceStore.java:64-67,308-311` |
| B28 | Busy-Wait-Polling statt `monitor`; bei `timeout==0` **Endlosschleife** | `AsyncNodeExecutor.java:78-100`, `WorkflowScheduler.java:55-64` |
| B29 | Async-Kind kann doppelt laufen (`runChildNode` → Fallback `runner.runNode`) | `AsyncNodeExecutor.java:61-65` |
| B30 | Sandbox-stderr unbegrenzt (nur stdout gedrosselt) → OOM bei stderr-Flut | `SandboxPoolClient.java:132-160` |
| B31 | `GrepTool`: ReDoS (Pattern ohne Timeout) + `readAllLines` je Datei + naive Include-Glob | `GrepTool.java:71,84,95` |
| B32 | `FindTool`: leerer `catch {}`, sammelt kompletten Baum in-memory | `FindTool.java:79,88-94` |
| B33 | `MultiEditTool` verspricht „atomic", schreibt aber inkrementell (Teilzustand bei Fehler) | `MultiEditTool.java:36,81` |
| B34 | `ApplyPatchTool` fragil: `-` entfernt ersten `indexOf`, IOExceptions verschluckt, ADD überschreibt | `ApplyPatchTool.java:55-133` |
| B35 | `SystemController`: erfundene Metriken; JSON per `indexOf`/`substring` geparst | `:159-166,277-292` |
| B36 | `AutomationService.lastAssistantText` liefert Platzhalter-Ergebnis und persistiert ihn | `:174` (genutzt `:143`) |
| B37 | `WorkerMetrics.recordTaskFailed` inkrementiert anderen Zähler als registriert (`completed_total{status=failed}` vs. `failed_total`) | `WorkerMetrics.java:46-49,90-94` |
| B38 | Doppelte Beans (`GracefulShutdown`, `WorkerMetrics`) → nur wegen `allow-bean-definition-overriding: true` bootbar | `QuadWorkerConfig.java:428-455` |
| B39 | Kafka-Bootstrap-Defaults inkonsistent (`localhost:9092` Producer vs. `kafka:9092` Consumer); nicht-transaktionale Producer bei `read_committed` | `QuadWorkerConfig.java:52-58`, `KafkaConfig.java:26,61` |
| B40 | `worker.quad.concurrency: 3` gebunden, aber ungenutzt (echte Grenze `max-concurrent-tasks: 24`) | `application.yml:51`, `TaskExecutor.java:97` |

### 5.4 Niedrig / Kleinigkeiten

- `EmailInitiatorStore` Redis-Key-Typo `email:initator:` (`:29`) — auch in `IMPL_STATUS.md:414` „dokumentiert".
- `IntakeListener.handleIntake` ist ein No-op, der nur ackt (`:38-47`); baut zudem pro Nachricht einen neuen `ObjectMapper` (`:59`).
- `WorkflowTaskListener` erzeugt virtuellen Executor, der nie heruntergefahren wird (`:57`).
- `JournalService` Default `${QUAD_JOURNAL_PATH:logs/journal.jsonl}` wird von MicroProfile-Config **nicht** expandiert (`:26`).
- `GuardrailFactory.SQL_INJECTION` nutzt `--\s*$` ohne `MULTILINE` (`:23`) → wirkungslos; Guardrails insgesamt leicht umgehbar.
- `AutomationService.list` vergleicht mit Literal `"error"` statt `TaskRun.STATUS_ERROR` (`:44`).
- `OrchestratorLeaseService` ignoriert injizierte `Clock` (nutzt `Clock.systemUTC()` `:58`); ungenutzte `timestamp` `:101-103`.
- `AutoScalingService` berechnet `lagThreshold/min/maxWorkers`, nutzt sie nicht (`:56-58`).
- `CheckpointService.approve/reject` mutiert Checkpoint doppelt; `InMemoryCheckpointStore` mutiert in-place.

---

## 6. Unverständlicher / schwer wartbarer Code

- **`WorkflowGeneratorService` (776 Z.)** — LLM-I/O, JSON-Reparatur, DAG-Bau, Join-Synthese, Persistenz in einem Fluss.
- **`WorkflowEngine.onResult`** — UUID-Parsing, Stale-/Superseded-Erkennung, DAG-Reload, Decoding, 4-Wege-Switch, Retry/Eskalation, DLQ, HITL-Erzeugung in einer langen Methode.
- **`TaskExecutor.execute`** — Timeout-Clamping, Prompt-Bau, Session-Bindung, Semaphore-Async-Buchhaltung und drei `catch`-Blöcke; die Permit-Übergabe ist nur durch einen langen Kommentar `:229-244` verständlich.
- **`ApplyPatchTool`** — Zustandsmaschine mit Magic-`substring`-Offsets (`:55-133`).
- **`SystemController.extract`** — handgeschriebener JSON-Parser (`:277-292`).
- **`SandboxPoolClient.run`** — Docker-Aufruf + Timeout + Stream-Erfassung inkl. Ringpuffer und Magic-`substring(0,500)`.
- **`ConditionalNodeExecutor`** — dreimal derselbe „Downstream skip"-Block (`:63-73,78-82`) statt Helper.
- **`DeclarativeAgentWorkflow` (agentflow)** — `HashSet`/`HashMap` in Workflow-Code (Replay-Risiko), **nicht-transitives** Pruning (`:138-142`: nur direkte Nicht-Ziele werden gepruned, Nachfahren weder übersprungen noch ausgeführt), Loop-Default `Math.max(1, maxIterations())` → unset läuft genau einmal (`:149-171`).
- **`IntakeListener`**: drei Consumer-Groups auf einem Topic (echt / No-op / Tracking) ohne Diagramm verwirrend.
- **`SecretResolver`-Cache** speichert Maps als `\n`/`=`-verketteten String (`:144-154,206-216`) — bricht bei `=`/`\n` in Werten.

---

## 7. Vergleich mit Best Practices

| Bereich | Abweichung | Beispiel |
|---------|-----------|----------|
| **Eventing** | Zwei konkurrierende Event-Systeme in `quad-core` (direkte Listener + `SubmissionPublisher` + `EventConsumer`/`EventManager`) | `AgentEventPublisher.java:30-36` |
| **Logging** | Drei Frameworks in `quad-core` (JUL, JBoss Logging, SLF4J); `System.out`/`printStackTrace` in Bibliothekscode | `quad-core` 3/8/9 Dateien; `DynamicSubAgentTool.java:199,214,219`; `HITLPlugin.java:139`; `ConsoleChannel.java:26`; `QuadAgent.java:38` |
| **DI** | `@Inject protected`-Felder + Zustand (`Agent.java:39-53`); `@Value`-Felder im Konstruktor | `SecretResolver.java:58-78` |
| **State/Scope** | Mutable Collections auf `@ApplicationScoped` (`StateDiffMemoryBridge.java:9-11`), unbeschränkte Maps (`ProvenanceTracker.java:38-40`) | |
| **Threading** | Executors ohne Lifecycle (`@PreDestroy`), rohe Daemon-Threads für Runs | `WorkflowSupport.java:44`, `SchedulerService.java:40`, `SandboxPoolClient.java:69`, `AgentController` |
| **REST/DTO** | `Map<String,Object>`-Bodies ohne Validierung; DTOs mit public mutable fields; Schema-String leakt in `ApiDtos` | `WorkspaceResource.java:36-107` |
| **Fehlerbehandlung** | Leere `catch {}` (555 Catch-Blöcke gesamt), `throws Exception` (407×) | `FindTool.java:92`, `ApplyPatchTool.java:82`, `McpManagerService.java:62-64` |
| **Transaktionen** | Mehrstufige Deletes/Writes nicht-transaktional; Write-Through ohne Rollback | `SessionStore.deleteSession:139-152`, `SharedState`/`PersistenceStore` |
| **JSON** | Jackson 2 **und** 3 gemischt; fünf Map/ ObjectMapper-Instanzen in `quad-core` | `SecretResolver.java:3-4` vs. `tools.jackson.*` in `UnifiedApiGateway`/`EventJson` |
| **Config** | Globales `allow-bean-definition-overriding: true` verdeckt Fehler | `quad-workflow-worker` yml `:20` |
| **Frontend-Security** | API-Key + Tenant in `localStorage` (XSS-lesbar) | `tenant.interceptor.ts:8-9`, `settings.ts:64-65` |
| **Reproduzierbarkeit** | Sechs Specs/Status-Docs mit vier verschiedenen Testzahlen; `spec.md` als KI-Artefakt | siehe §9 |
| **Positive Abweichungen** | Durchgängig parametrisierte SQL (`?`), try-with-resources, Ports/Adapter, fail-closed Upcaster/Publish-Compiler | |

---

## 8. Modul-Steckbriefe

### 8.1 `quad-core` (238 Dateien, 16.513 LOC)
Starker Kern mit guten Teilen (`StuckDetector`, `ConversationManager`/`HistoryCompactor`, `ReflectiveToolMethod`, zweiphasiges Skill-Loading, `ToolMeta`). Hauptprobleme: zwei Event-Systeme, zwei `Capability`-Typen, God-Class `Agent`/`AgentRuntime`, ~9 tote + 12 test-only Klassen, mehrere echte Bugs (§5.3 B15-B22), Fragmentierung beim Logging.

### 8.2 `quad-quarkus` (605 Dateien)
Gute Ports/Adapter und ein solider Schema-Validator/Upcaster. Aber: tote v2-Pipeline (Import ohne semantische Validierung, §4.2), God-Services, **zwei** sicherheitsrelevante Pfad-/SSRF-Lücken (§5.2 B3-B5), verwaiste `src/test/tmp`-Tests, doppelte `AuditStore`/`HookRegistry`.

### 8.3 `kafka-platform` (199 Dateien)
Echte, deterministische DAG-Engine mit Flyway/Checkpoints/HITL/DLQ/JWT-Gerüst. Aber: kritischer E-Mail-Pfad tot (B1), ack-vor-Publish (B2), invertierte Vault-Health (B7), JWT-401-Maskierung (B10), sechs „fertige", aber toten Features (§4.3), extreme Doku-Drift (§9), `agent-worker` ist ein paralysierter Legacy-Mock neben dem echten `quad-workflow-worker`.

### 8.4 `agentflow` (129 Dateien)
Temporal-Setup mit korrekten deterministischen Grundzügen, aber: Generator ist divergierte Kopie ohne Join (A1/B6), Plan ohne Token-Limit (B11), `RequestContext`-Leck (B12), Replay-/Pruning-/Loop-Fragen im Declarative-Workflow (§6), mehrere Workflow-Impl-Stubs (Dead-Code-Kandidaten).

### 8.5 `integration-tests`
**104 `@Test`** (README nennt „58"), **3 `@Disabled`-Befunde** (README nennt „2"), und die **komplette `syscapability`-Suite (46 Tests)** fehlt in der README-Tabelle. Fixed `Thread.sleep`-Polling und hartkodierte Ports → Flakiness. Positiv: das `@Disabled`-Befund + Begleittest-Muster.

### 8.6 Frontends
- `quad-ui/WorkflowComposer.tsx`: **1.669 Zeilen, 26 `useState`** (`:226-565`), vier `useEffect` — Canvas, Inspector, Run-Panel, Backend-Wahl und Materialisierung in einer Datei.
- `quad-ui/src/services/api.ts`: `fetchSkills` (`:437`) und `fetchSkillList` (`:504`) byte-identisch; `createAgent`/`updateAgent` duplizieren einen 19-Feld-`flat`-Block.
- `agentflow-ui/graph-editor.ts` (593 Z.) monolithisch; `agentflow-ui` speichert API-Key/Tenant in `localStorage` (XSS-Risiko). `quad-ui` nutzt **kein** `localStorage` für Secrets — das ist die bessere Linie.

---

## 9. Dokumentation & Wissensschulden

### 9.1 Drift (kritisch für Vertrauen)

| Dokument | Problem |
|----------|---------|
| `kafka-platform/README.md` | Paket `com.agentplatform.*`; „Jackson 3 only" (Code mischt 2+3); Testzahl „359" |
| `kafka-platform/IMPLEMENTATION_COMPLETE.md` | nennt `tracing/TracingProducerInterceptor.java` (existiert nicht; real `TraceHeaders.java`); Testzahlen 41/84; „P0–P6 complete" |
| `kafka-platform/IMPL_STATUS.md` | E-Mail-Responder/Vault/Alerting „implemented" (alles unerreichbar, §4.3); reproduziert den `email:initator:`-Typo |
| `kafka-platform/TODO.md` | Paket `com.agentplatform.common.event`; behauptet „kein Spring AI/LLM" — `quad-workflow-worker` besteht daraus |
| `kafka-platform/SPEC2.md` | Spring Boot **3.3**, `com.platform.*`, Module `agent-domain`/`agent-alerting`/`agent-dlq-replay` — Code ist Boot 4, `de.augmentia.*` |
| `kafka-platform/spec.md` | **kein Dokument**: Python-Traceback, `"""`, `print(...)`, Einrückungsfehler, Spec doppelt (1-793 & 795+) |
| `kafka-platform/TEST.md` | falscher Projektpfad; „Confluent 7.7"; Modell `deepseek-v4`; Metrikname, den der Failure-Pfad nicht emittiert |

### 9.2 Vom Team selbst dokumentierte Lücken (extrahiert)

Aus `quad-quarkus/Luecken.md` (untracked): `SkillRegistry` tot; `deactivate` entfernt alle Skills; stille Skill-Parse-Fehler; **`ToolExecutor` defaultet `ALLOW_ALL`** + `NOOP`-Audit, `RoleBasedToolGuard`/`JsonlAuditLogger` nie verdrahtet; Idempotency-Key via `hashCode()`; kein HTTP-429-Backoff; keine Context-Window-Steuerung; ThreadLocal `CurrentSession` nie geleert; ein globales Lock; Tool-Spans als Roots; `StopReason` immer `COMPLETED`; `gen_ai.system` hart `"openai"`; `-parameters` fehlt in POMs.

Aus `quad-quarkus/Optional_Improvements.md` (tracked): Agent-`model` persistiert, aber nicht angewendet; fehlende Per-Definition-Steuerung (Skills, MCP, Compaction, ReAct-Limit, Kostenlimit, Sub-Agents, Tool-Blocklist); flache DTO-Konstruktoren akkretieren Felder (Feature-Map als Lösung).

Aus `TODO2.md` (untracked) / Integrationstests (als `@Disabled`): `KafkaTopicInitializer` nur create-if-missing (keine Reconcile bestehender Topics); quad akzeptiert leeren Workflow (`nodes: []`); Nebenläufigkeitslücke; Config-Fallen (hartkodierter Worker-Port 8092, CWD-relativer `data/quad.db`).

### 9.3 Scratch-Dokumente (Rot-Risiko)
`kafka-platform/{IMPLEMENTATION_COMPLETE,IMPL_STATUS,NEW,TODO,TEST,UI,spec,SPEC2,quad-core-worker}.md`; `agentflow/{Ideas,DECISION_NODE_COMPARISON}.md`. Zwei konkurrierende Statusdokumente (`IMPLEMENTATION_COMPLETE.md` vs. `IMPL_STATUS.md`) sind widersprüchlich. `TODO2.md` (918 Z.) ist ein vollwertiger Issue-Tracker, aber **nicht versioniert** — die Befunde existieren nur auf einer Festplatte.

---

## 10. Priorisierte Empfehlungen

**Sofort (Korrektheit/Sicherheit)**
1. E-Mail-Pipeline reparieren oder entfernen (B1) — auf `WorkflowStartEvent` + Korrelationsfeld vereinheitlichen; E2E-Test.
2. Publish vor Ack bestätigen (B2): `send(...).get()` oder Shutdown-Flush.
3. `PrefixSourceResolver` auf Workspace confine + SSRF-Block (B3); `sessionId` an jeder FS-/Docker-Grenze validieren/normalisieren (B4); `symlinkSafe` ohne lexikalischen Fallback (B5).
4. Vault-Wiring korrigieren oder Vault-Bridge entfernen (B7/B8).
5. JWT-Filter: `chain.doFilter` außerhalb des `try` (B10); Slack-Signing verifizieren (B14).

**Kurzfristig (Architektur)**
6. `WorkflowGeneratorService` teilen (gemeinsames Modul) und Join-Logik nachziehen (A1/B6), inkl. Fork→Merge-Regressionstest.
7. v2-Transferpipeline verdrahten (semantische Validierung + Upcaster + Publish) oder löschen (B23/A6).
8. Doku konsolidieren: **ein** Statusdokument pro Modul, `spec.md` löschen, Testzahlen aus CI erzeugen, `TODO2.md` → getrackte Issues (A4/§9).
9. Monorepo-weites CI für `quad-quarkus` + `agentflow` + Root-Reaktor (A4).
10. GDPR-Endpunkte implementieren oder als Stub kennzeichnen (B13); Metriknamen korrigieren (B37).

**Mittelfristig (Hygiene)**
11. Toten Code entfernen (§4); ein Logging-Framework + JSON-Helper statt String-Konkatenation (B25/§7).
12. `quad-core`: Event-Systeme vereinheitlichen, `Capability`-Typen umbenennen, `Agent`/`AgentRuntime` zerlegen.
13. `WorkflowComposer.tsx` und `graph-editor.ts` aufteilen; API-Client-Duplikate entfernen; `agentflow-ui` ohne `localStorage`-Secrets.
14. Integrationstests: `Awaitility` statt `Thread.sleep`, Ports aus Config, `stackMussLaufen` deduplizieren, README-Zahlen angleichen.

---

## 11. Was bereits gut ist (bewahren)

- **Stufen-Narrativ** und „Was ändert sich"-Tabelle im Root-README.
- **Ports/Adapter** in der Workflow-Engine; einzelner Wiring-Point (`WorkflowEngineProducer`).
- **`QuadWorkflowSchemaValidator` / `QuadWorkflowUpcaster` / `QuadWorkflowPublishCompiler`** — deterministisch, fail-closed, kanonisches Hashing; nur schade, dass die Pipeline nicht verdrahtet ist.
- **`InputBuilder`** mit Unresolved-Placeholder-Reporting und Strict-Render.
- **Sandbox-Sicherheits-Defaults** (`--network none`, `--rm`, Memory-Cap, Stub-Fallback, Ringpuffer).
- **`@Disabled`-Befund + Begleittest**-Muster in `integration-tests`.
- **Ehrliche Selbst-Dokumentation** (`Luecken.md`, `Optional_Improvements.md`) — wertvoller als die meisten Backlogs; sollte nur getrackt/aktualisiert werden.
- **Keine getrackten Secrets**, sauberes `.gitignore` (node_modules/target/workspace/logs), nur `.example`-Templates; `quad-ui` ohne `localStorage`-Secrets.

---

*Kein Datei-Änderungsbedarf während des Reviews außer dieser `Review.md`. Aussagen sind mit `Datei:Zeile` belegt; „kein Aufrufer gefunden" ≠ bewiesen tot. Für einen Tier-3-Audit sollten die toten-Kandidaten (§4.4) und die Drift-Dokumente (§9) mit vollständiger Pagination und beidseitiger Aufrufrichtung nachgezogen werden.*
