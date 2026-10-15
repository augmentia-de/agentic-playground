package de.augmentia.quad.core.tool.builtin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.augmentia.quad.core.annotation.Param;
import de.augmentia.quad.core.annotation.Tool;
import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.CurrentSession;
import de.augmentia.quad.core.session.WorkspaceResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@ApplicationScoped
public class GrepTool {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int DEFAULT_MAX_RESULTS = 250;
    /** Overall wall-clock budget for a single search; guards against catastrophic regex backtracking. */
    private static final long DEFAULT_TIMEOUT_MS = 5000L;
    /** Files larger than this are skipped instead of being read fully into memory. */
    private static final long MAX_FILE_BYTES = 10L * 1024 * 1024;
    private static final Set<String> BINARY_EXTENSIONS = Set.of(
            ".jar", ".class", ".so", ".zip", ".gz", ".tar", ".png", ".jpg", ".jpeg", ".gif",
            ".bmp", ".exe", ".dll", ".dylib", ".bin", ".pdf", ".woff", ".woff2", ".ttf", ".o", ".a");

    @Inject
    protected WorkspaceResolver workspaceResolver;

    @Inject
    protected CurrentSession currentSession;

    protected AgentSessionState currentState() {
        return currentSession != null ? currentSession.get() : CurrentSession.getCurrent();
    }

    protected WorkspaceResolver getWorkspaceResolver() {
        return workspaceResolver;
    }

    public void setWorkspaceResolver(WorkspaceResolver resolver) {
        this.workspaceResolver = resolver;
    }

    /** Thrown when a regex match exceeds the search deadline (ReDoS guard). */
    static final class RegexTimeoutException extends RuntimeException {
        RegexTimeoutException() { super("regex timeout"); }
    }

    /**
     * CharSequence wrapper that aborts a match once the deadline is exceeded; the regex engine
     * calls {@link #charAt(int)} during backtracking, so this reliably interrupts catastrophic patterns.
     */
    static final class DeadlineCharSequence implements CharSequence {
        private final CharSequence delegate;
        private final long deadlineNanos;

        DeadlineCharSequence(CharSequence delegate, long deadlineNanos) {
            this.delegate = delegate;
            this.deadlineNanos = deadlineNanos;
        }

        @Override
        public int length() {
            return delegate.length();
        }

        @Override
        public char charAt(int index) {
            if (System.nanoTime() > deadlineNanos) {
                throw new RegexTimeoutException();
            }
            return delegate.charAt(index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return new DeadlineCharSequence(delegate.subSequence(start, end), deadlineNanos);
        }

        @Override
        public String toString() {
            return delegate.toString();
        }
    }

    @Tool("Search for a pattern within files. Supports regex.")
    public String grepSearch(
            @Param("pattern") String pattern,
            @Param(value = "path", required = false) String path,
            @Param(value = "include", required = false) String include,
            @Param(value = "caseSensitive", required = false) Boolean caseSensitive,
            @Param(value = "maxResults", required = false) Integer maxResults) {

        var state = currentState();
        if (state == null) {
            return "Error: No active session found";
        }

        if (pattern == null || pattern.isBlank()) {
            return "Error: Search pattern is required";
        }

        int max = (maxResults == null || maxResults <= 0) ? DEFAULT_MAX_RESULTS : maxResults;
        Path searchRoot;
        try {
            searchRoot = (path != null && !path.isBlank())
                    ? getWorkspaceResolver().resolveForRead(state, path)
                    : getWorkspaceResolver().workspaceDir();
        } catch (SecurityException e) {
            return "Access denied: " + e.getMessage();
        }

        int flags = Boolean.TRUE.equals(caseSensitive) ? 0 : Pattern.CASE_INSENSITIVE;
        Pattern regexPattern;
        try {
            regexPattern = Pattern.compile(pattern, flags);
        } catch (Exception e) {
            return "Error: Invalid pattern: " + e.getMessage();
        }

        Pattern includePattern = compileIncludePattern(include);
        long deadlineNanos = System.nanoTime() + timeoutMs() * 1_000_000L;

        StringBuilder results = new StringBuilder();
        int matchCount = 0;

        try (Stream<Path> walk = Files.walk(searchRoot)) {
            List<Path> files = walk.filter(Files::isRegularFile).toList();
            for (Path file : files) {
                if (matchCount >= max) break;
                String fileName = file.getFileName().toString();
                if (includePattern != null && !includePattern.matcher(fileName).matches()) {
                    continue;
                }
                if (isBinaryByExtension(fileName)) {
                    continue;
                }
                long size;
                try {
                    size = Files.size(file);
                } catch (Exception e) {
                    continue;
                }
                if (size > MAX_FILE_BYTES) {
                    continue;
                }
                byte[] bytes;
                try {
                    bytes = Files.readAllBytes(file);
                } catch (Exception e) {
                    continue;
                }
                if (looksBinary(bytes)) {
                    continue;
                }
                String content = new String(bytes, StandardCharsets.UTF_8);
                String[] lines = content.split("\\R", -1);
                for (int i = 0; i < lines.length && matchCount < max; i++) {
                    Matcher matcher = regexPattern.matcher(new DeadlineCharSequence(lines[i], deadlineNanos));
                    boolean found;
                    try {
                        found = matcher.find();
                    } catch (RegexTimeoutException tex) {
                        return "Error during grep: timeout";
                    }
                    if (found) {
                        Path relPath = searchRoot.relativize(file);
                        results.append(relPath).append(":").append(i + 1).append(": ").append(lines[i].trim()).append("\n");
                        matchCount++;
                    }
                }
            }
        } catch (Exception e) {
            if (e instanceof RegexTimeoutException) {
                return "Error during grep: timeout";
            }
            return "Error during grep: " + e.getMessage();
        }

        ObjectNode json = MAPPER.createObjectNode();
        json.put("pattern", pattern);
        json.put("total", matchCount);

        if (matchCount == 0) {
            return "No matches found for pattern: " + pattern;
        }

        var matchesArray = json.putArray("matches");
        for (String line : results.toString().split("\n")) {
            if (!line.isBlank()) {
                matchesArray.addObject().put("text", line);
            }
        }

        return results.append(json.toPrettyString()).toString();
    }

    private static long timeoutMs() {
        String prop = System.getProperty("quad.grep.timeoutMs");
        if (prop != null && !prop.isBlank()) {
            try {
                long v = Long.parseLong(prop.trim());
                if (v > 0) return v;
            } catch (NumberFormatException ignored) {
                // fall through to default
            }
        }
        return DEFAULT_TIMEOUT_MS;
    }

    private static boolean isBinaryByExtension(String fileName) {
        String lower = fileName.toLowerCase(Locale.ROOT);
        int dot = lower.lastIndexOf('.');
        return dot >= 0 && BINARY_EXTENSIONS.contains(lower.substring(dot));
    }

    private static boolean looksBinary(byte[] bytes) {
        int limit = Math.min(8192, bytes.length);
        for (int i = 0; i < limit; i++) {
            if (bytes[i] == 0) {
                return true;
            }
        }
        return false;
    }

    private static Pattern compileIncludePattern(String include) {
        if (include == null || include.isBlank()) {
            return null;
        }
        StringBuilder globRegex = new StringBuilder("^");
        for (int i = 0; i < include.length(); i++) {
            char c = include.charAt(i);
            if (c == '*') {
                globRegex.append(".*");
            } else if (c == '?') {
                globRegex.append(".");
            } else {
                globRegex.append(Pattern.quote(String.valueOf(c)));
            }
        }
        globRegex.append("$");
        try {
            return Pattern.compile(globRegex.toString());
        } catch (Exception e) {
            return null;
        }
    }
}
