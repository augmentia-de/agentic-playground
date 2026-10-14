package de.augmentia.quad.workflow.engine.node;

import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.WorkspaceResolver;
import de.augmentia.quad.workflow.engine.internal.WorkflowExecutionContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PrefixSourceResolverTest {

    private final AgentSessionState state = new AgentSessionState();
    private final WorkflowExecutionContext ctx = new WorkflowExecutionContext("run", null, "", "",
            state, false, null, true);

    @AfterEach
    void clearAllowedRoots() {
        System.clearProperty("quad.prefix.allowed-roots");
    }

    @Test
    void bareStringStaysAsIs() {
        assertEquals("static block", PrefixSourceResolver.resolve("static block", ctx, "n1"));
    }

    @Test
    void inlineJsonElementIsSerialized() {
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put("title", "Block A");
        raw.put("items", List.of("a", "b"));
        String resolved = PrefixSourceResolver.resolve(raw, ctx, "n1");
        assertEquals("{\"title\":\"Block A\",\"items\":[\"a\",\"b\"]}", resolved);
    }

    @Test
    void stepReferenceResolvesOutputOfReferencedNode() {
        ctx.putOutput("n1", "step-1 result");
        Map<String, Object> src = Map.of("step", "n1");
        assertEquals("step-1 result", PrefixSourceResolver.resolve(src, ctx, "n2"));
    }

    @Test
    void missingStepReferenceResolvesNull() {
        Map<String, Object> src = Map.of("step", "ghost");
        assertNull(PrefixSourceResolver.resolve(src, ctx, "n2"));
    }

    @Test
    void selfReferenceResolvesOwnNodeOutput() {
        ctx.putOutput("n5", "self result");
        Map<String, Object> src = new HashMap<>();
        src.put("self", true);
        assertEquals("self result", PrefixSourceResolver.resolve(src, ctx, "n5"));
    }

    @Test
    void fileSourceReadsAbsolutePathInsideAllowedRoot(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("prefix.md");
        Files.writeString(file, "file block lorem ipsum");
        System.setProperty("quad.prefix.allowed-roots", dir.toString());
        String resolved = PrefixSourceResolver.resolve("file://" + file, ctx, "n1");
        assertEquals("file block lorem ipsum", resolved);
    }

    @Test
    void absoluteFileOutsideAllowedRootsIsRejected(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("secret.md");
        Files.writeString(file, "should not be readable");
        // dir is deliberately NOT registered as an allowed root
        assertNull(PrefixSourceResolver.resolve("file://" + file, ctx, "n1"));
    }

    @Test
    void relativeFileIsReadInsideWorkspace() throws Exception {
        Path sessionRoot = Path.of(WorkspaceResolver.resolve(state.getSessionId()));
        Files.createDirectories(sessionRoot);
        Path file = sessionRoot.resolve("rel.md");
        Files.writeString(file, "relative block");
        try {
            assertEquals("relative block", PrefixSourceResolver.resolve("file://rel.md", ctx, "n1"));
        } finally {
            Files.deleteIfExists(file);
            Files.deleteIfExists(sessionRoot);
        }
    }

    @Test
    void relativeTraversalOutsideWorkspaceIsRejected() {
        assertNull(PrefixSourceResolver.resolve(
                "file://../../../../etc/hostname", ctx, "n1"));
    }

    @Test
    void symlinkEscapeFromWorkspaceIsRejected(@TempDir Path outside) throws Exception {
        Path sessionRoot = Path.of(WorkspaceResolver.resolve(state.getSessionId()));
        Files.createDirectories(sessionRoot);
        Path secret = outside.resolve("secret.md");
        Files.writeString(secret, "symlink secret");
        Path link = sessionRoot.resolve("escape");
        Files.deleteIfExists(link);
        Files.createSymbolicLink(link, outside);
        try {
            assertNull(PrefixSourceResolver.resolve("file://escape/secret.md", ctx, "n1"));
        } finally {
            Files.deleteIfExists(link);
            Files.deleteIfExists(sessionRoot);
        }
    }

    @Test
    void missingFileResolvesNull() {
        assertNull(PrefixSourceResolver.resolve("file:///does/not/exist.md", ctx, "n1"));
    }

    @Test
    void invalidUrlFailsSilently() {
        assertNull(PrefixSourceResolver.resolve("https://invalid.invalid/nope", ctx, "n1"));
    }

    @Test
    void loopbackUrlIsRejected() {
        assertNull(PrefixSourceResolver.resolve("http://127.0.0.1:9/x", ctx, "n1"));
        assertNull(PrefixSourceResolver.resolve("http://localhost:9/x", ctx, "n1"));
    }

    @Test
    void linkLocalMetadataUrlIsRejected() {
        assertNull(PrefixSourceResolver.resolve(
                "http://169.254.169.254/latest/meta-data/", ctx, "n1"));
    }

    @Test
    void ipv6LoopbackUrlIsRejected() {
        assertNull(PrefixSourceResolver.resolve("http://[::1]:9/x", ctx, "n1"));
    }

    @Test
    void nonHttpUrlFieldIsRejected() {
        Map<String, Object> src = Map.of("url", "ftp://example.com/x");
        assertNull(PrefixSourceResolver.resolve(src, ctx, "n1"));
        Map<String, Object> fileUrl = Map.of("url", "file:///etc/passwd");
        assertNull(PrefixSourceResolver.resolve(fileUrl, ctx, "n1"));
    }

    @Test
    void mapUrlFieldFetches() {
        // "url" key with a non-URL value -> silent null (no crash)
        Map<String, Object> src = Map.of("url", "not a url");
        assertNull(PrefixSourceResolver.resolve(src, ctx, "n1"));
    }

    @Test
    void tooLargeSourceIsCapped() {
        // "static block, then filler" -> inline string is used as-is (no cap below threshold)
        String big = "x".repeat(300_000);
        String resolved = PrefixSourceResolver.resolve(big, ctx, "n1");
        assertEquals(300_000, resolved.length());
    }
}
