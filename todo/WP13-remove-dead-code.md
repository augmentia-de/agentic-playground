# WP13 — Remove dead code

| | |
|---|---|
| **Status** | open |
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

- [ ] Every candidate: proof documented.
- [ ] All builds green.
- [ ] No "implemented" claim in the docs without reachable code (synchronised with WP14).
- [ ] Progress log with a deletion list (file → reason).

## Progress log

| Date | Author | Removed | Proof |
|------|--------|---------|-------|
| | | | |
