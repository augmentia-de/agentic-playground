package de.augmentia.quad.workflow.engine.node;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.quad.core.session.WorkspaceResolver;
import de.augmentia.quad.workflow.engine.internal.WorkflowExecutionContext;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Resolves a node's {@code prefixSource} into concrete prefix text for the
 * agent-scoped prefix chain (§6). Sources, resolved at runtime:
 * <ul>
 *   <li><b>inline JSON element</b> — a bare string (used as-is) or any JSON tree
 *       element (serialized to text);</li>
 *   <li><b>URL</b> — {@code {"url": "https://..."}} or a bare
 *       {@code http(s)://} string, fetched at runtime (size-capped). Only public hosts are
 *       allowed; loopback/private/link-local targets and redirects are refused (SSRF);</li>
 *   <li><b>file</b> — {@code {"file": "path"}} or a bare {@code file://} string;
 *       relative paths are resolved against (and confined to) the session workspace,
 *       absolute paths are only read below the workspace base or a configured
 *       {@code quad.prefix.allowed-roots} entry (symlink-safe, size-capped);</li>
 *   <li><b>step reference</b> — {@code {"step": "<nodeId>"}} or {@code {"self": true}}
 *       resolves to the referenced node's output (requires it to have executed,
 *       DAG order; otherwise the source resolves to null = no prefix).</li>
 * </ul>
 */
public final class PrefixSourceResolver {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_SOURCE_CHARS = 256 * 1024;
    /** Extra absolute roots allowed for file prefix sources (comma/whitespace separated). */
    private static final String ALLOWED_ROOTS_PROPERTY = "quad.prefix.allowed-roots";
    private static final String ALLOWED_ROOTS_ENV = "QUAD_PREFIX_ALLOWED_ROOTS";
    /** Redirects are refused: a redirect would bypass the SSRF host check below. */
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();

    private PrefixSourceResolver() {
    }

    /**
     * Resolves the raw {@code prefixSource} config value into prefix text, or
     * {@code null}/blank when nothing usable can be produced (unresolvable step,
     * fetch/read error, absent config).
     */
    @SuppressWarnings("unchecked")
    public static String resolve(Object raw, WorkflowExecutionContext ctx, String nodeId) {
        if (raw == null) return null;
        if (raw instanceof String s) return resolveString(s, ctx);
        if (raw instanceof Map<?, ?> m) return resolveMap((Map<String, Object>) m, ctx, nodeId);
        return serialize(raw);
    }

    private static String resolveString(String s, WorkflowExecutionContext ctx) {
        String t = s.trim();
        if (t.startsWith("https://") || t.startsWith("http://")) {
            return fetchUrl(t);
        }
        if (t.startsWith("file://")) {
            return readFile(t.substring("file://".length()), ctx);
        }
        return s;
    }

    private static String resolveMap(Map<String, Object> m, WorkflowExecutionContext ctx, String nodeId) {
        if (m.containsKey("url")) {
            return fetchUrl(String.valueOf(m.get("url")));
        }
        if (m.containsKey("file")) {
            return readFile(String.valueOf(m.get("file")), ctx);
        }
        if (m.containsKey("step")) {
            return ctx.output(String.valueOf(m.get("step")));
        }
        if (Boolean.TRUE.equals(m.get("self"))) {
            return ctx.output(nodeId);
        }
        return serialize(m);
    }

    private static String serialize(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            return String.valueOf(value);
        }
    }

    /**
     * Fetches a URL into prefix text. Only {@code http}/{@code https} to public hosts is
     * allowed; requests to loopback/private/link-local/unique-local addresses are refused
     * (SSRF hardening). Redirects are not followed.
     */
    private static String fetchUrl(String url) {
        try {
            URI uri = URI.create(url);
            String scheme = uri.getScheme();
            if (scheme == null
                    || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
                return null;
            }
            if (isBlockedHost(uri.getHost())) {
                return null;
            }
            HttpRequest request = HttpRequest.newBuilder(uri)
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();
            HttpResponse<byte[]> response = HTTP.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() < 200 || response.statusCode() >= 300) return null;
            return cap(new String(response.body(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean isBlockedHost(String host) {
        if (host == null || host.isBlank()) {
            return true;
        }
        String h = host;
        if (h.startsWith("[") && h.endsWith("]")) {
            h = h.substring(1, h.length() - 1); // URI.getHost() keeps IPv6 brackets
        }
        try {
            for (InetAddress addr : InetAddress.getAllByName(h)) {
                if (addr.isLoopbackAddress() || addr.isAnyLocalAddress()
                        || addr.isLinkLocalAddress() || addr.isSiteLocalAddress()
                        || addr.isMulticastAddress() || isUniqueLocalIpv6(addr)) {
                    return true;
                }
            }
        } catch (UnknownHostException e) {
            return true; // unresolvable -> refuse
        }
        return false;
    }

    private static boolean isUniqueLocalIpv6(InetAddress addr) {
        byte[] b = addr.getAddress();
        return b.length == 16 && (b[0] & 0xfe) == 0xfc; // fc00::/7
    }

    /**
     * Reads a file prefix source. Relative paths are confined to the session workspace
     * ({@code base/{sessionId}}); absolute paths are only allowed below the workspace base or an
     * explicitly configured allowed root. All checks are real-path based (symlink-safe).
     * Returns {@code null} when the path is not permitted or cannot be read.
     */
    private static String readFile(String path, WorkflowExecutionContext ctx) {
        try {
            if (path == null || path.isBlank()) {
                return null;
            }
            Path candidate = Path.of(path);
            Path target;
            if (candidate.isAbsolute()) {
                target = confinedAbsolute(candidate);
                if (target == null) {
                    return null;
                }
            } else {
                String sessionId = ctx.sessionState() != null ? ctx.sessionState().getSessionId() : "default";
                Path sessionRoot = Path.of(WorkspaceResolver.resolve(sessionId));
                Path resolved = Path.of(WorkspaceResolver.resolve(sessionId, path));
                Path realRoot = WorkspaceResolver.realPath(sessionRoot);
                Path realTarget = WorkspaceResolver.realPath(resolved);
                if (!realTarget.startsWith(realRoot)) {
                    return null;
                }
                target = realTarget;
            }
            return cap(Files.readString(target, StandardCharsets.UTF_8));
        } catch (IOException | InvalidPathException | SecurityException e) {
            return null;
        }
    }

    /**
     * Returns the real path of {@code absolute} if it is contained in one of the allowed roots,
     * otherwise {@code null}.
     */
    private static Path confinedAbsolute(Path absolute) {
        Path real = WorkspaceResolver.realPath(absolute);
        for (Path root : allowedRoots()) {
            if (real.startsWith(WorkspaceResolver.realPath(root))) {
                return real;
            }
        }
        return null;
    }

    private static List<Path> allowedRoots() {
        List<Path> roots = new ArrayList<>();
        roots.add(Path.of(WorkspaceResolver.WORKSPACE_BASE));
        String extra = System.getProperty(ALLOWED_ROOTS_PROPERTY);
        if (extra == null) {
            extra = System.getenv(ALLOWED_ROOTS_ENV);
        }
        if (extra != null) {
            for (String part : extra.split("[,\\s]+")) {
                if (!part.isBlank()) {
                    roots.add(Path.of(part));
                }
            }
        }
        return roots;
    }

    private static String cap(String s) {
        if (s == null) return null;
        return s.length() <= MAX_SOURCE_CHARS ? s : s.substring(0, MAX_SOURCE_CHARS);
    }
}