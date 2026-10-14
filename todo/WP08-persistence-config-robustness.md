# WP08 — Persistence & config robustness

| | |
|---|---|
| **Status** | open |
| **Severity** | Medium |
| **Modules** | `quad-quarkus/quad-quarkus` |
| **Review reference** | B26, B27, B35, B36, §5.4 (JournalService) |
| **Depends on** | – |
| **Estimated effort** | 2 days |

## Goal

Persistence survives individual corrupt records and loses no values; no invented metrics/results are
reported externally; JSON is not parsed via substring.

## Findings (as-is)

- **B26 – loading aborts on one row:** `PersistenceStore.loadAllWorkflows`/`loadAllAgents` wrap the
  entire `while (rs.next())` loop in one `try/catch` (`:94-192`) → one broken row (`:175-178`)
  discards all the rest.
- **B27 – lossy CSV arrays:** arrays are comma-joined (`:64-67`), `splitArr` (`:308-311`) cannot
  reconstruct values containing commas; the same pattern in `security/AuditStore`.
- **B35 – invented metrics / fragile parser:** `SystemController.getMetrics`/`getErrorMetrics`
  return fake numbers (`:159-166`); JSON parsing via `indexOf`/`substring` (`:277-292`).
- **B36 – placeholder result:** `AutomationService.lastAssistantText` returns
  `"[session-… transcript]"` and persists that as the result (`:174`, used at `:143`).
- **§5.4 – `JournalService` default** `${QUAD_JOURNAL_PATH:logs/journal.jsonl}` is not expanded (`:26`).

## 1. Tests first (Red)

Test location: `quad-quarkus/quad-quarkus/src/test/java/de/augmentia/quad/quarkus/…`
(existing: `persistence/RunStoreTest`, `persistence/AgentStepExporterTest`,
`persistence/TelemetryStoreTest`).

| # | Test | Expectation |
|---|------|-------------|
| 1 | `PersistenceStoreTest#oneBadRow_othersStillLoad` | broken row skipped, rest loaded |
| 2 | `PersistenceStoreTest#arrayWithComma_roundTrips` | value with a comma preserved |
| 3 | `PersistenceStoreTest#arrayEmptyAndSpecialChars` | `[]`, `"a,b"`, special chars correct |
| 4 | `SystemControllerTest#metrics_areReal_orMarkedStub` | real numbers or explicitly marked as stub |
| 5 | `SystemControllerTest#extract_nestedJson` | nested JSON parsed correctly |
| 6 | `AutomationServiceTest#resultText_isRealOrEmpty` | real result or empty, no placeholder text |
| 7 | `JournalServiceTest#defaultPathIsExpanded` | default path is a real path |

**Acceptance (Red):** 1, 2, 4, 6, 7 fail.

## 2. Changes (Green)

1. **`loadAll*`:** make the row loop resilient (log the error per row + `continue`); keep the DB access
   try-with-resources.
2. **Arrays:** a JSON column instead of CSV (or an escaping format); tolerate old CSV values during
   migration.
3. **`SystemController`:** real metrics from existing stores OR a clear `stub` marker (field
   `"stub": true`); replace `extract` with `ObjectMapper` (`JsonNode`).
4. **`AutomationService`:** replace the placeholder with a real result or an empty value.
5. **Config:** convert `${QUAD_JOURNAL_PATH:...}` into a default evaluated in Java (or property
   registration) so the literal placeholder never serves as a path.

## 3. Verification

```bash
cd quad-quarkus && mvn -pl quad-quarkus test
cd quad-quarkus && mvn -pl quad-quarkus test -Dtest='*Persistence*,SystemController*,Automation*'
```
- Populate the DB with manually corrupted `nodes` JSON → the app starts with the remaining inventory.

## 4. Risks / notes

- Schema/format change for arrays → provide an upcaster/migration.
- Fake metrics might be expected by the UI → plan the UI change (see WP12).

## Definition of Done

- [ ] Tests 1–7 green.
- [ ] Robust recovery on single-row errors.
- [ ] Lossless array persistence.
- [ ] No fake metrics/placeholder results externally.
- [ ] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| | | 1. Tests red | |
| | | 2. Implementation | |
| | | 3. Verification | |
