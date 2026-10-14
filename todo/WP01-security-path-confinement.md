# WP01 — Security: path confinement & SSRF

| | |
|---|---|
| **Status** | done |
| **Severity** | Critical |
| **Modules** | `quad-quarkus/quad-core`, `quad-quarkus/quad-workflow-engine`, `quad-quarkus/quad-sandbox-docker` |
| **Review reference** | B3, B4, B5 (§5.2) |
| **Depends on** | – |
| **Estimated effort** | 1–2 days |

## Goal

Every place where a workflow, a session id, or a URL derived from user input leads to a
filesystem or network access is restricted to a **tight, validated base**.
No escape from the workspace, no reading arbitrary host files, no SSRF.

## Findings (as-is)

- **B3a – arbitrary host file read:** `PrefixSourceResolver.readFile` treated every path starting
  with `/` as an absolute host path (`PrefixSourceResolver.java:108-121`).
  `prefixSource: {"file": "/etc/passwd"}` ended up in the prompt.
- **B3b – SSRF:** `PrefixSourceResolver.fetchUrl` fetched any `http(s)` URL including redirects,
  without an allow-list/private-IP block (`:40`, `:94-106`).
- **B4 – unvalidated session id:** `WorkspaceResolver.sessionDir(id) = base.resolve(id)` (`:46-48`),
  the static `resolve(id)` was pure string concatenation (`:36-38`). Absolute ids / `..` escaped the
  base; `SandboxPoolClient.workspaceDir` passed that into an **rw** bind mount
  (`SandboxPoolClient.java:262-265,105-106`).
- **B5 – symlink confinement defeated:** `WorkspaceResolver.symlinkSafe` returned the target when the
  real path escaped but the lexical path was inside (`:161-172`); likewise
  `symlinkSafeAgainstGrant` (`:174-189`).

## 1. Tests first (Red)

### 1.1 `PrefixSourceResolver` (`quad-workflow-engine`)

File: `quad-quarkus/quad-workflow-engine/src/test/java/de/augmentia/quad/workflow/engine/node/PrefixSourceResolverTest.java` (exists).

| Test | Expectation |
|------|-------------|
| `absoluteFileOutsideAllowedRootsIsRejected` | absolute path outside the allowed roots → `null` (no content) |
| `fileSourceReadsAbsolutePathInsideAllowedRoot(@TempDir)` | absolute path inside `quad.prefix.allowed-roots` → content |
| `relativeTraversalOutsideWorkspaceIsRejected` | `../../../../etc/hostname` → `null` |
| `symlinkEscapeFromWorkspaceIsRejected` | symlink inside the workspace pointing outside → `null` |
| `relativeFileIsReadInsideWorkspace` | normal relative file in the session workspace → content |
| `loopbackUrlIsRejected` | `http://127.0.0.1`, `http://localhost` → `null` |
| `linkLocalMetadataUrlIsRejected` | `http://169.254.169.254/…` → `null` |
| `ipv6LoopbackUrlIsRejected` | `http://[::1]` → `null` |
| `nonHttpUrlFieldIsRejected` | `{"url":"ftp://…"}` / `{"url":"file:///etc/passwd"}` → `null` |

> No WireMock was needed: the SSRF guard rejects loopback/private hosts **before** any request is
> made, so the tests assert rejection without a server.

### 1.2 `WorkspaceResolver` (`quad-core`; new)

File (new): `quad-quarkus/quad-core/src/test/java/de/augmentia/quad/core/session/WorkspaceResolverTest.java`
(`WorkspaceResolver` actually lives in `quad-core`, not `quad-workflow-engine`).

| Test | Expectation |
|------|-------------|
| `sanitizeSessionIdAcceptsSafeIds` | `null`/blank → `default`; `sess-1`, `abc-123_x.y`, `wf:step:0:1:abcdef12` unchanged |
| `sanitizeSessionIdRejectsTraversal` | `..`, `.`, `a/b`, `a\b`, `/abs`, NUL/control → `SecurityException` |
| `staticResolveConfinesToSessionRoot` | relative sub-path stays below the session root |
| `staticResolveRejectsEscapes` | `../../etc/passwd`, `/etc/passwd`, `../evil` → `SecurityException` |
| `sessionDirRejectsTraversal` | `../outside` → `SecurityException` |
| `resolveForReadAllowsSessionFile` | normal file inside the session → path |
| `resolveForReadRejectsSymlinkEscape` | symlink escaping the session → `SecurityException` |
| `resolveForWriteRejectsSymlinkEscape` | symlink escaping the session (write) → `SecurityException` |
| `realPathResolvesExistingSymlink` / `realPathAppendsNonExistingSuffix` | real-path helper behaviour |

### 1.3 Sandbox mount (extended)

File: `quad-quarkus/quad-sandbox-docker/src/test/java/…/SandboxPoolClientTest.java`

| Test | Expectation |
|------|-------------|
| `resolvesDefaultWorkspaceForNullSession` | `null` id → `…/default` (existing behaviour preserved) |
| `rejectsTraversalSessionId` | `run("echo hi", "../../etc")` → `"invalid session id"`, no docker call |

**Acceptance (Red):** all new tests fail (or pin today's misbehaviour) before the change.

## 2. Changes (Green)

1. **Central containment helper:** `WorkspaceResolver` gained `sanitizeSessionId`, hardened static
   `resolve(id)`/`resolve(id, subPath)`, and the instance `sessionDir(id)` now validates the id.
   `symlinkSafe`/`symlinkSafeAgainstGrant` use a new real-path helper (`realNearest`/`realPath`)
   with **no lexical fallback**.
2. **Session-id validation:** an id must be usable as a single path segment — it must not contain
   a path separator (`/`, `\`), NUL or another control character, and must not be exactly `.`/`..`;
   otherwise `SecurityException`. Other printable characters (e.g. `:` used by the Kafka worker) are
   allowed. Central helper so the sandbox/prefix code share it.
3. **`PrefixSourceResolver`:**
   - relative paths are confined to the session workspace (real-path check, symlink-safe);
   - absolute paths are only read below the workspace base or a configured
     `quad.prefix.allowed-roots` (sysprop or `QUAD_PREFIX_ALLOWED_ROOTS`, comma/whitespace separated);
   - `fetchUrl` only allows `http`/`https` to public hosts, blocks loopback/any-local/link-local/
     site-local/multicast/IPv6 unique-local, and uses `HttpClient.Redirect.NEVER`.
4. **`SandboxPoolClient`:** resolves the workspace via the validating `WorkspaceResolver.resolve`;
   an invalid id fails closed with an error string instead of mounting a traversing path.
5. **`PromptAssembler`:** no longer concatenates a raw session id into the prompt fallback.

## 3. Verification

```bash
cd quad-quarkus && mvn -pl quad-core test -Dtest='WorkspaceResolverTest,GrantedDirectoryTest'
cd quad-quarkus && mvn -pl quad-workflow-engine -am test -Dtest='PrefixSourceResolverTest' -Dsurefire.failIfNoSpecifiedTests=false
cd quad-quarkus && mvn -pl quad-sandbox-docker -am test -Dtest='SandboxPoolClientTest' -Dsurefire.failIfNoSpecifiedTests=false
cd quad-quarkus && mvn -T1C test -fae   # full reactor regression
```

- Manual negative check: workflow with `prefixSource.file=/etc/hostname` → no content in the prompt.
- Reviewed remaining `Files.read*`/`Path.of(` calls in the touched modules.

## 4. Risks / notes

- `toRealPath()` throws for non-existent targets → solved with a "nearest existing ancestor"
  strategy (`realNearest`) that re-appends the non-existing suffix while still detecting
  symlinked parent directories.
- Windows path/`\` specifics are handled lexically by the id pattern (no separators allowed).
- Absolute file prefix sources that were previously read from anywhere now require an explicit
  `quad.prefix.allowed-roots` entry → document the property for operators.

## Definition of Done

- [x] All test cases from §1 present and green.
- [x] No absolute outer boundary reachable; central containment helper introduced.
- [x] `symlinkSafe` fallback removed.
- [x] SSRF allow-list/private-IP block implemented (host-based; documented).
- [x] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| 2026-10-14 | @opencode | 1. Tests (Red) | 10 + 18 + 3 new/updated cases added; `quad-core` 10+6, `quad-workflow-engine` 18, `quad-sandbox-docker` 3 green after change |
| 2026-10-14 | @opencode | 2. Implementation | `WorkspaceResolver` (sanitize/confine/realPath), `PrefixSourceResolver` (file confinement + SSRF), `SandboxPoolClient`, `PromptAssembler` |
| 2026-10-14 | @opencode | 3. Verification | targeted module tests green; full `quad-quarkus` reactor regression run |
