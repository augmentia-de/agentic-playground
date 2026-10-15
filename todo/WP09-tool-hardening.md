# WP09 — Tool hardening

| | |
|---|---|
| **Status** | done |
| **Severity** | Medium |
| **Modules** | `quad-quarkus/quad-tool-builtin`, `quad-quarkus/quad-workflow-*` (tool classes) |
| **Review reference** | B31, B32, B33, B34 (§5.3) |
| **Depends on** | WP01 (containment helper) |
| **Estimated effort** | 2 days |

## Goal

File/search tools are robust against DoS, lose no errors, do not half-modify files, and fulfil their
documented guarantees (e.g. "atomic").

## Findings (as-is)

- **B31 – GrepTool:** pattern without a timeout (ReDoS), `readAllLines` per file, naive include glob
  (`GrepTool.java:71,84,95`).
- **B32 – FindTool:** empty `catch {}`, collects the whole tree in memory (`:79,88-94`).
- **B33 – MultiEditTool:** documents "atomic" but writes incrementally; partial state on error
  (`MultiEditTool.java:36,81`).
- **B34 – ApplyPatchTool:** fragile; `-` removes the first `indexOf`, IOExceptions swallowed, ADD
  overwrites (`ApplyPatchTool.java:55-133`).

## 1. Tests first (Red)

Test locations per tool (new if not present):
`quad-quarkus/quad-tool-builtin/src/test/java/de/augmentia/quad/core/tool/…`

| # | Test | Expectation |
|---|------|-------------|
| 1 | `GrepToolTest#catastrophicPattern_timesOut` | malicious pattern → timeout, no hang |
| 2 | `GrepToolTest#binaryFiles_skipped` | `.jar`/`.class`/`.so` are skipped |
| 3 | `GrepToolTest#includeGlob_escapesRegexMeta` | `*.txt` does not match `xtxt` etc. |
| 4 | `FindToolTest#unreadableDir_logsAndContinues` | error reported, not swallowed |
| 5 | `MultiEditToolTest#failure_leavesNoPartialEdits` | failure → no partial change (or clear "best effort" docs) |
| 6 | `ApplyPatchToolTest#removalUsesContext` | `-` with context removes the correct position |
| 7 | `ApplyPatchToolTest#addRefusesExisting` | ADD to an existing file → error |
| 8 | `ApplyPatchToolTest#ioErrorPropagated` | IOException is propagated |

**Acceptance (Red):** 1–8 fail or expose the misbehaviour.

## 2. Changes (Green)

1. **GrepTool:** search timeout/limited matching (line/byte limit), extend binary detection, escape the
   include glob correctly (`Pattern.quote` for literals).
2. **FindTool:** log the `catch`; result stream/`limit` instead of the full in-memory tree.
3. **MultiEditTool:** either real atomicity (validate all, then write; backup/rollback) or correct the
   docs/name ("best effort").
4. **ApplyPatchTool:** make the parser robust (consider context, report conflicts), check overflow/edge
   cases, propagate I/O errors, refuse ADD collisions.
5. **Path safety:** all file access via the containment helper from WP01.

## 3. Verification

```bash
cd quad-quarkus && mvn -pl quad-tool-builtin test
cd quad-quarkus && mvn -pl quad-quarkus test -Dtest='*Tool*'
```
- Consider fuzz/property tests for the patch parser.

## 4. Risks / notes

- Behaviour changes in MultiEdit/ApplyPatch can affect agent prompts → update the tool descriptions.
- Make the timeout value configurable.

## Definition of Done

- [x] Tests 1–8 green.
- [x] No ReDoS/no hang in Grep.
- [x] Errors no longer swallowed.
- [x] Tool guarantees match the implementation.
- [x] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| 2026-10-15 | agent | 1. Tests red | `GrepToolTest`, `FindToolTest#unreadableDir_logsAndContinues`, `MultiEditToolTest#failure_leavesNoPartialEdits`, `ApplyPatchToolTest` (3) — 7 failing |
| 2026-10-15 | agent | 2. Implementation | Grep: deadline-based ReDoS guard (`quad.grep.timeoutMs`, 5 s default), binary detection (extension + NUL), safe include-glob→regex, size cap. Find: `walkFileTree` + `visitFileFailed` logs & continues, `limit` instead of full tree. MultiEdit: two-phase validate-all-then-write with rollback. ApplyPatch: context-based removal, ADD refuses existing, UPDATE requires existing, I/O errors propagated. |
| 2026-10-15 | agent | 3. Verification | `mvn -pl quad-tool-builtin test` → 27 green; `mvn -pl quad-quarkus -am test` → BUILD SUCCESS (532/201/75/27/20/10 tests green) |

