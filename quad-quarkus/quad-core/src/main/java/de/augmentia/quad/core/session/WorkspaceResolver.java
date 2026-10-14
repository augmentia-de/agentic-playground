package de.augmentia.quad.core.session;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

@ApplicationScoped
public class WorkspaceResolver {

    /** Defaults to the process CWD ('.') so agents work in the current directory. */
    public static final String WORKSPACE_BASE = Path.of(System.getProperty("quad.workspace",
        System.getenv().getOrDefault("QUAD_WORKSPACE", "."))).toAbsolutePath().normalize().toString();

    /**
     * A session id must be usable as a <em>single</em> path segment below the workspace base.
     * It therefore must not contain a path separator ({@code "/"}, {@code "\"}), NUL or any
     * other control character, and must not be exactly {@code "."} or {@code ".."}.
     * Other printable characters (e.g. {@code ":"} as used by the Kafka worker) are permitted,
     * because they cannot traverse and stay a single segment on POSIX.
     */
    private static final int MAX_SESSION_ID_LENGTH = 255;

    @Inject
    CurrentSession currentSession;

    @ConfigProperty(name = "quad.workspace", defaultValue = ".")
    Path baseWorkspace;

    /**
     * Fluent override of the workspace base, for programmatic (non-CDI) use.
     * A {@code null} base falls back to {@link #WORKSPACE_BASE}.
     */
    public WorkspaceResolver baseWorkspace(Path base) {
        this.baseWorkspace = base;
        return this;
    }

    /**
     * Validates a session id for use as a single path segment below the workspace base.
     *
     * @param sessionId the raw id; {@code null}/blank resolves to {@code "default"}
     * @return the trimmed, validated id
     * @throws SecurityException if the id is {@code "."}/{@code ".."}, is too long, or contains a
     *         path separator, NUL or another control character
     */
    public static String sanitizeSessionId(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) {
            return "default";
        }
        String id = sessionId.trim();
        if (id.equals(".") || id.equals("..")) {
            throw new SecurityException("Access denied: invalid session id '" + sessionId + "'");
        }
        if (id.length() > MAX_SESSION_ID_LENGTH) {
            throw new SecurityException("Access denied: session id too long");
        }
        for (int i = 0; i < id.length(); i++) {
            char c = id.charAt(i);
            if (c == '/' || c == '\\' || c == '\0' || Character.isISOControl(c)) {
                throw new SecurityException("Access denied: invalid session id '" + sessionId + "'");
            }
        }
        return id;
    }

    /**
     * Absolute session root (workspace base + validated session id).
     *
     * @throws SecurityException if the session id is invalid
     */
    public static String resolve(String sessionId) {
        return Path.of(WORKSPACE_BASE).resolve(sanitizeSessionId(sessionId)).normalize().toString();
    }

    /**
     * Resolves a <em>relative</em> sub-path below the session root.
     *
     * @throws SecurityException if the path is absolute or escapes the session root
     */
    public static String resolve(String sessionId, String subPath) {
        Path sessionRoot = Path.of(resolve(sessionId));
        Path target = sessionRoot.resolve(subPath == null ? "" : subPath).normalize();
        if (!target.startsWith(sessionRoot)) {
            throw new SecurityException(
                "Access denied: '" + subPath + "' escapes the session workspace.");
        }
        return target.toString();
    }

    public Path sessionDir() {
        AgentSessionState current = currentSession != null ? currentSession.get() : CurrentSession.getCurrent();
        String sessionId = current != null ? current.getSessionId() : "default";
        return sessionDir(sessionId);
    }

    public Path sessionDir(String sessionId) {
        return baseWorkspace().resolve(sanitizeSessionId(sessionId)).normalize();
    }

    public Path sessionDir(String sessionId, String subPath) {
        return sessionDir(sessionId).resolve(subPath);
    }

    /**
     * The workspace root (= the configured {@code quad.workspace} base). Tools use this as the
     * default directory for path-less calls (e.g. {@code findFiles(pattern)},
     * {@code listDirectory()}) so they operate on the workspace instead of the per-session subdir.
     */
    public Path workspaceDir() {
        return baseWorkspace();
    }

    /**
     * Resolves the current workflow directory (CWD) for the given session state.
     * <p>
     * The result is always confined to the session root: {@code base/{sessionId}}.
     * A {@code currentCwd()} of {@code "."} resolves to the session root itself;
     * {@code pushCwd("backend")} resolves to {@code base/{sessionId}/backend}.
     * Paths that escape the session root (e.g. via {@code ".."}) are rejected
     * with a {@link SecurityException}.
     *
     * @param state the session state carrying the CWD stack
     * @return the absolute, normalized CWD path within the session root
     */
    public Path cwdDir(AgentSessionState state) {
        Path sessionRoot = sessionDir(state.getSessionId()).normalize();
        String current = state.currentCwd() != null ? state.currentCwd() : ".";
        Path cwd = sessionRoot.resolve(current).normalize();
        if (!cwd.startsWith(sessionRoot)) {
            throw new SecurityException(
                "Access denied: CWD escapes session workspace: " + current);
        }
        return cwd;
    }

    /**
     * Resolves a sub-path against the current workflow directory.
     * Guards against path traversal outside the session root.
     *
     * @param state   the session state carrying the CWD stack
     * @param subPath the path relative to the current CWD
     * @return the absolute, normalized path within the session root
     */
    public Path cwdDir(AgentSessionState state, String subPath) {
        Path sessionRoot = sessionDir(state.getSessionId()).normalize();
        Path target = cwdDir(state).resolve(subPath).normalize();
        if (!target.startsWith(sessionRoot)) {
            throw new SecurityException(
                "Access denied: Path resides outside of session workspace: " + subPath);
        }
        return target;
    }

    /**
     * Resolves a path for read access, allowing both the session workspace
     * (with CWD) and any user-granted directories.
     * <p>
     * Only grant directories with at least {@link Access#READ} are permitted.
     * The session root itself is always readable.
     *
     * @param state   the session state carrying CWD and granted directories
     * @param subPath the path (absolute or relative to current CWD)
     * @return the absolute, normalized path that is safe to access
     * @throws SecurityException if the path is outside the workspace and all granted dirs
     */
    public Path resolveForRead(AgentSessionState state, String subPath) {
        Path sessionRoot = sessionDir(state.getSessionId()).normalize();
        Path target = resolveAgainstCwd(state, subPath);

        if (target.startsWith(sessionRoot)) {
            return symlinkSafe(target, sessionRoot);
        }
        if (state.accessFor(target) != null) {
            Access access = state.accessFor(target);
            return symlinkSafeAgainstGrant(target, access, state);
        }
        throw new SecurityException(
            "Access denied: '" + subPath + "' is outside the session workspace and all granted directories.");
    }

    /**
     * Resolves a path for write access. Only the session workspace (always
     * writable) and granted directories with {@link Access#READ_WRITE} are
     * permitted. Read-only granted dirs are rejected for writes.
     *
     * @throws SecurityException if the path is not writable
     */
    public Path resolveForWrite(AgentSessionState state, String subPath) {
        Path sessionRoot = sessionDir(state.getSessionId()).normalize();
        Path target = resolveAgainstCwd(state, subPath);

        if (target.startsWith(sessionRoot)) {
            return symlinkSafe(target, sessionRoot);
        }
        Access access = state.accessFor(target);
        if (access == Access.READ_WRITE) {
            return symlinkSafeAgainstGrant(target, access, state);
        }
        throw new SecurityException(
            "Access denied: '" + subPath + "' is not writable (either outside the workspace or only read-only granted).");
    }

    private Path resolveAgainstCwd(AgentSessionState state, String subPath) {
        return cwdDir(state).resolve(subPath).normalize();
    }

    /**
     * Real-path containment check against the session root to prevent symlink escapes.
     * Uses a real-path comparison on both the target and the root; there is deliberately
     * <em>no</em> lexical fallback, because that would let a symlink escape inside a
     * lexically-contained path.
     */
    private Path symlinkSafe(Path target, Path root) {
        Path realTarget = realNearest(target);
        Path realRoot = realNearest(root);
        if (realTarget.startsWith(realRoot)) {
            return target;
        }
        throw new SecurityException(
            "Access denied: real path of '" + target + "' escapes the session workspace (symlink).");
    }

    private Path symlinkSafeAgainstGrant(Path target, Access access, AgentSessionState state) {
        Path realTarget = realNearest(target);
        for (GrantedDirectory gd : state.grantedDirectories()) {
            if (realTarget.startsWith(realNearest(gd.path()))) {
                return target;
            }
        }
        throw new SecurityException(
            "Access denied: real path of '" + target + "' does not reside in any granted directory (symlink).");
    }

    /**
     * Real (symlink-resolved) path of the nearest existing ancestor of {@code p}, with the
     * non-existing suffix re-appended. This is correct for targets that do not exist yet while
     * still detecting symlinked parent directories (which a plain {@code toRealPath()} on the
     * full path could not, because it fails for non-existent targets).
     */
    static Path realNearest(Path p) {
        Path abs = p.toAbsolutePath().normalize();
        Path existing = abs;
        Path suffix = null;
        while (existing != null && !Files.exists(existing, LinkOption.NOFOLLOW_LINKS)) {
            Path name = existing.getFileName();
            if (name != null) {
                suffix = suffix == null ? name : name.resolve(suffix);
            }
            existing = existing.getParent();
        }
        if (existing == null) {
            return abs;
        }
        Path realBase;
        try {
            realBase = existing.toRealPath();
        } catch (IOException e) {
            realBase = existing.toAbsolutePath().normalize();
        }
        return suffix == null ? realBase : realBase.resolve(suffix).normalize();
    }

    /**
     * Public real-path helper for callers outside this class (e.g. workflow prefix resolution).
     * Never returns {@code null}.
     */
    public static Path realPath(Path p) {
        return realNearest(p);
    }

    private Path baseWorkspace() {
        Path base = baseWorkspace != null ? baseWorkspace : Path.of(WORKSPACE_BASE);
        return base.toAbsolutePath().normalize();
    }
}
