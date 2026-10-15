# WP13 — Remove dead code

| | |
|---|---|
| **Status** | done |
| **Severity** | Medium |
| **Modules** | all |
| **Review reference** | A5, §4.1–§4.4 |
| **Depends on** | **after WP05–WP11** (some code is still touched there) |
| **Estimated effort** | 2 days |

## Goal

Code that never executes is removed or clearly marked as experimental/test.
No "implemented" promise without a reachable call path.

## Approach

> **Important:** justify every deletion with a dead-code proof first
> (graph `trace_path` inbound + `grep -rn`), not just "looks unused".

Per candidate: proof → delete/relocate → build + tests green → changelog entry.

## Findings (as-is, selection)

### quad-core (concretely dead)
`GuardrailException`, `QuadEventStore`, `SessionLockService` (also functionless: empty
`LockEntry.refresh`, no-op `heartbeat`, `:40,46`), `AgentMemory`, `MultiSystemMessageChatMemory`,
`SummarizingConversationManager`, `SagaAwareConversationManager` (no-op `:11-15`),
`StateDiffMemoryBridge` (unsynchronised, never cleared `:9-11`), `ToolSetChangeNotifier`.

**Test-wired only:** `EventQuery`, `InspectInputsPrefill`, `TextOnlyGuard`,
`StructuredOutputValidator`, `TokenRecovery`, `QuadConfig`, `RunSnapshot`,
`SlidingWindowConversationManager`, `PredictStrategy`, `ReflexionStrategy`, `TemplateStrategy`,
`TimerChannel` → **keep or move into the test module**, not production `main`.

### quad-quarkus
- **v2 transfer/publish pipeline dead:** `QuadWorkflowPublishCompiler`, `QuadWorkflowUpcaster`,
  `QuadWorkflowSemanticValidator`, `QuadWorkflowHitlNormalizer` only in tests/docs —
  **either** wire them into `WorkflowTransferService` (see note below) **or** remove.
- **`quad-quarkus/src/test/tmp/`** (5 E2E files) is **not compiled** (no `build-helper`,
  no POM reference) → either move to `src/test/java` or delete.
- File/class mismatch: `NooaWorkflowSchemaValidatorTest.java` declares
  `class QuadWorkflowSchemaValidatorTest`.
- Dead methods: `AgentController.matches*` (`:736-769`), `McpManagerService.bridgeToSystemProperties`
  (`:241-249`), `WorkflowEngine.onMessage/hasPendingHandler` (`:189-199`),
  `AgentStepExporter.reset` (`:98-102`), `AutomationService.scheduledPrompt` (`:176-179`);
  dead field `SandboxPoolClient.executor` (`:48,69`).

### kafka-platform (docs say "done", unreachable)
`SlackNotifier`, `PagerDutyAlertSender`, `SchemaRegistryClient` (log no-op), `VaultConfig`,
`SecretResolver` (if removed in WP02), `PrefixCache`, `MailResponseHandler` (inert).

### agentflow
Several parallel `*WorkflowImpl`/stub classes (`QuadSagaWorkflow`, `SagaWorkflowImpl`,
`RichAgentWorkflowImpl`, `HitlWorkflowImpl`, `AuthorizedAgentWorkflowImpl`, …) →
**dead-code candidates**; verify first (Temporal registration/callers), then decide.

## 1. Tests first

- For each candidate: a proof script/note (graph + `grep`).
- Ensure the module test suite stays green after removal (no hidden reflection calls → `grep` for the
  class name as a string).
- **Special case v2 pipeline:** if it is to be **wired** rather than deleted, that belongs in its own
  package (test of semantic validation on import), not here.

## 2. Changes (Green)

1. Delete concretely dead code (file + references + tests that only check the declaration).
2. Move test-only code from `main` into test sources.
3. Resolve `src/test/tmp` (move/delete).
4. Fix the name mismatch.
5. For every removal, delete the **doc claim** in the Md files (otherwise the docs lie) → align with WP14.

## 3. Verification

```bash
mvn -T1C clean install
cd quad-quarkus && mvn test
cd kafka-platform && mvn verify
cd agentflow && mvn test
```
- `grep -rn "<RemovedClass>"` → no hits (except the changelog).

## 4. Risks / notes

- Reflection/SPI/Jackson polymorphism can make "dead" code look alive → verify before deleting.
- **Do not** delete anything an earlier WP still needs (hence the dependency).

## Definition of Done

- [x] Every candidate: proof documented.
- [x] All builds green.
- [x] No "implemented" claim in the docs without reachable code (docs mirroring handed to WP14, see below).
- [x] Progress log with a deletion list (file → reason).

## Result

Every candidate was re-proved against the current tree (`grep -rn` over `*.java/*.xml/*.properties/*.yaml/*.yml/*.json/*.txt`, plus the Maven build).
Several review entries were **false positives** and were deliberately kept:

- `GuardrailException` — **live**: thrown at `guardrail/GuardrailPlugin.java` (`BlockAction.THROW`) and caught in `hook/pipeline/HookRegistry`.
- `SlidingWindowConversationManager` — **live**: base class of `SummarizingSlidingWindowConversationManager`, used by `ConversationManager`/`Agent`/`AgentRuntime`.
- `QuadWorkflowPublishCompiler` and `QuadWorkflowSemanticValidator` — **live**: used by agentflow `WorkflowDefinitionLoader`.
- `WorkflowEngine.onMessage` — **live**: called by `MessagingRouter`.
- `AgentController.matchesKind/Status` — the *live* copies are in `SystemController`; only the private `AgentController` duplicates were removed.
- kafka `SecretResolver`, `MailResponseHandler` — **intentional opt-in bridge components**, pinned by the `ComponentScanCoverageTest` invariant (`@ConditionalOnProperty`, `matchIfMissing=false`); `MailResponseHandler` is an active `@KafkaListener`. Kept.
- agentflow `*WorkflowImpl` — **live**: `@WorkflowImpl(taskQueues="agentflow")` (`io.temporal.spring.boot`) classes, auto-registered via `spring.temporal.workers-auto-discovery` and driven by `AgentFlowController` (`newWorkflowStub`). Kept.

## Verification

```bash
mvn -pl quad-quarkus -am test                                   # BUILD SUCCESS (all quad modules)
mvn -pl kafka-platform/agent-orchestrator,kafka-platform/agent-worker -am test
# no dangling references:
grep -rn "QuadEventStore\|SessionLockService\|StateDiffMemoryBridge\|ToolSetChangeNotifier" .
```

## Follow-ups for WP14 (docs mirroring)

The following kafka docs still advertise the removed classes and must be corrected
(they also carry the wrong `com.agentplatform.*` package names — WP14 item):
`kafka-platform/TODO.md`, `kafka-platform/IMPL_STATUS.md`, `kafka-platform/IMPLEMENTATION_COMPLETE.md`.

## Progress log

| Date | Author | Removed | Proof |
|------|--------|---------|-------|
| 2026-10-15 | | quad-core — deleted `QuadEventStore`, `SessionLockService`, `AgentMemory`, `MultiSystemMessageChatMemory`, `SummarizingConversationManager`, `SagaAwareConversationManager`, `StateDiffMemoryBridge`, `ToolSetChangeNotifier` | repo-wide `grep` (all file types) → 0 refs outside the class itself; no tests |
| 2026-10-15 | | quad-core — moved `EventQuery`, `InspectInputsPrefill`, `TextOnlyGuard`, `StructuredOutputValidator`, `TokenRecovery`, `QuadConfig`, `RunSnapshot`, `PredictStrategy`, `ReflexionStrategy`, `TemplateStrategy`, `TimerChannel` from `src/main/java` to `src/test/java` (test-only) | each referenced only by its own unit test |
| 2026-10-15 | | quad-quarkus — removed dead members: `AgentController.matchesPeriod/matchesKind/matchesStatus/matchesRunId`, `McpManagerService.bridgeToSystemProperties`, `WorkflowEngine.hasPendingHandler` (+ transitive `WorkflowSupport.hasPendingHandlerFor`), `AgentStepExporter.reset`, `AutomationService.scheduledPrompt`, field `SandboxPoolClient.executor` | private/package-private with no callers (`grep -rn`); only the `WorkflowEngine` wrapper removed, `onMessage` kept |
| 2026-10-15 | | quad-quarkus — moved `QuadWorkflowUpcaster` + `QuadWorkflowHitlNormalizer` to test scope; moved `src/test/tmp/*` (5 E2E files) to `src/test/java/.../itest/`; renamed `NooaWorkflowSchemaValidatorTest.java` → `QuadWorkflowSchemaValidatorTest.java` | only-test refs; E2E are `@Tag`-gated (`e2e`/`llm`, excluded by default) and now compile |
| 2026-10-15 | | kafka-platform — deleted `SlackNotifier`, `PagerDutyAlertSender`, `VaultConfig`, `PrefixCache`; moved `SchemaRegistryClient` to test scope | 0 refs / own test only; not covered by the component-scan invariant |
| 2026-10-15 | | agentflow — **none removed** | `*WorkflowImpl` are `@WorkflowImpl` auto-discovered and reached from `AgentFlowController` |
