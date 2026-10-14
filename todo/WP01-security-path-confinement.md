# WP01 — Sicherheit: Pfad-Containment & SSRF

| | |
|---|---|
| **Status** | offen |
| **Schwere** | Kritisch |
| **Module** | `quad-quarkus/quad-workflow-engine`, `quad-quarkus/quad-quarkus` |
| **Review-Referenz** | B3, B4, B5 (§5.2) |
| **Abhängigkeit** | – |
| **Geschätzter Aufwand** | 1–2 Tage |

## Ziel

Alle Stellen, an denen ein Workflow, eine Session-ID oder eine URL aus Nutzereingabe zu einem
Dateisystem- oder Netzwerkzugriff führt, werden auf eine **enge, validierte Basis** begrenzt.
Kein Ausbruch aus dem Workspace, kein Lesen beliebiger Host-Dateien, kein SSRF.

## Befunde (Ist-Zustand)

- **B3a – willkürliches Host-Datei-Lesen:** `PrefixSourceResolver.readFile` behandelt jeden mit `/`
  beginnenden Pfad als absoluten Hostpfad (`PrefixSourceResolver.java:108-121`).
  `prefixSource: {"file": "/etc/passwd"}` landet im Prompt.
- **B3b – SSRF:** `PrefixSourceResolver.fetchUrl` lädt jede `http(s)`-URL inkl. Redirects, ohne
  Allow-List/Private-IP-Block (`:40`, `:94-106`).
- **B4 – Session-ID ungeprüft:** `WorkspaceResolver.sessionDir(id) = base.resolve(id)` (`:46-48`),
  statische `resolve(id)` ist reine Konkatenation (`:36-38`). Absolute IDs / `..` entkommen der Basis;
  `SandboxPoolClient.workspaceDir` reicht das in einen **rw**-Bind-Mount (`SandboxPoolClient.java:262-265,105-106`).
- **B5 – Symlink-Confine ausgehebelt:** `WorkspaceResolver.symlinkSafe` gibt Ziel zurück, wenn realer
  Pfad entkommt, aber lexikalisch innen liegt (`:161-172`); analog `symlinkSafeAgainstGrant` (`:174-189`).

## 1. Tests zuerst (Red)

### 1.1 `PrefixSourceResolver` (`quad-workflow-engine`)

Datei: `quad-quarkus/quad-workflow-engine/src/test/java/de/augmentia/quad/workflow/engine/node/PrefixSourceResolverTest.java` (existiert).

| Test | Erwartung |
|------|-----------|
| `readFileAbsoluteHostPath_isRejected` | Input `/etc/passwd` → Exception/Skip, **kein** Inhalt |
| `readFileTraversal_isRejected` | Input `../../etc/passwd` → Exception/Skip |
| `readFileSymlinkEscape_isRejected` | Symlink im Workspace nach außen → Exception/Skip |
| `readFileInsideWorkspace_isAllowed` | normale relative Datei → Inhalt |
| `fetchUrlPrivateIp_isRejected` | `http://169.254.169.254/…`, `http://10.0.0.1`, `http://127.0.0.1` → abgelehnt |
| `fetchUrlRedirectToPrivateIp_isRejected` | Redirect öffentlich→privat → abgelehnt |
| `fetchUrlAllowedHost_isFetched` | Allow-List-Host → Inhalt (gemockt) |

> `fetchUrl`-Tests mit `WireMock` oder `MockWebServer` (Test-Dependency prüfen/hinzufügen).

### 1.2 `WorkspaceResolver` (neu)

Datei (neu): `quad-quarkus/quad-workflow-engine/src/test/java/de/augmentia/quad/workflow/engine/workspace/WorkspaceResolverTest.java`
(oder im Modul, in dem `WorkspaceResolver` liegt — per `grep -rl 'class WorkspaceResolver'` bestimmen).

| Test | Erwartung |
|------|-----------|
| `sessionDirAbsoluteId_isRejected` | `sessionId="/tmp/x"` → Exception |
| `sessionDirTraversal_isRejected` | `".."`, `"a/../../b"` → Exception |
| `sessionDirValid_isContained` | normaler Slug → Pfad liegt unter `base` |
| `symlinkSafe_escapingSymlink_isRejected` | Symlink → Exception |
| `symlinkSafe_internalSymlink_isAllowed` | Symlink innerhalb → ok |

### 1.3 Sandbox-Mount (neu/erweitert)

Datei: `quad-quarkus/quad-sandbox-docker/src/test/java/…/SandboxPoolClientTest.java`
→ sicherstellen, dass die Docker-Argumentliste **nur** validierte, enthaltene Pfade enthält
(mockbare Ausführungsmethode nutzen; sonst Refactoring vorsehen).

**Abnahme (Red):** alle neuen Tests fehlschlagen bzw. dokumentieren das heutige Fehlverhalten.

## 2. Änderungen (Green)

1. **Zentrale Containment-Helfer** (eine Stelle, nicht drei):
   `WorkspaceResolver` bekommt `Path resolveForRead(String sessionId, String path)` und
   `Path resolveForWrite(...)`, die normalisieren, `toRealPath()`-nah prüfen und
   **nur** innerhalb `realRoot` zulassen. Der lexikalische Fallback in `symlinkSafe` entfällt.
2. **Session-ID-Validierung:** `sessionDir(String id)` validiert `id` gegen ein Muster
   (`^[A-Za-z0-9._-]{1,64}$`, keine `.`/`..`), Sonst `IllegalArgumentException`.
   Zentrale Konstante, damit auch `SandboxPoolClient` und `PrefixSourceResolver` sie nutzen.
3. **`PrefixSourceResolver`:** absolute Pfade und Traversal über `resolveForRead` abbilden;
   `fetchUrl` mit Host-Allow-List (konfigurierbar), Block von Loopback/Private/Link-Local,
   Redirects manuell prüfen (`HttpClient.Redirect.NEVER` + eigene Zielprüfung).
4. **`SandboxPoolClient`:** vor dem Mount `sessionDir`/`resolveForWrite` verwenden; keine rohe
   Konkatenation.
5. **Konfiguration dokumentieren:** neue Properties (z. B. `quad.prefix.url-allowlist`).

## 3. Verifikation

```bash
cd quad-quarkus && mvn -pl quad-workflow-engine test
cd quad-quarkus && mvn -pl quad-sandbox-docker test
cd quad-quarkus && mvn -pl quad-quarkus test -Dtest='*Workspace*,*Prefix*,E2E_*'
```
- Manueller Negativtest: Workflow mit `prefixSource.file=/etc/hostname` → kein Inhalt im Prompt.
- Review der übrigen `Files.read*`/`Path.of(`-Aufrufe im Modul (`grep -rn "Path.of(\|Files.read"`).

## 4. Risiken / Hinweise

- `toRealPath()` wirft bei nicht existierenden Zielen; Schreibpfade brauchen eine
  „nächstes-existierendes-Elternverzeichnis"-Strategie.
- Windows-Pfad-/`\`-Sonderfälle (falls relevant) beachten.

## Definition of Done

- [ ] Alle Testfälle aus §1 vorhanden und grün.
- [ ] Keine absolute Außengrenze mehr erreichbar; central containment helper eingeführt.
- [ ] `symlinkSafe`-Fallback entfernt.
- [ ] SSRF-Allow-List konfigurierbar und dokumentiert.
- [ ] Fortschrittslog gepflegt.

## Fortschrittslog

| Datum | Bearbeiter | Schritt | Ergebnis |
|-------|-----------|---------|----------|
| | | Konzept | |
| | | 1. Tests rot | |
| | | 2. Umsetzung | |
| | | 3. Verifikation | |
