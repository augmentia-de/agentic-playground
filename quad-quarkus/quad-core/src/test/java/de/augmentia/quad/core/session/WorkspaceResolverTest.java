package de.augmentia.quad.core.session;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkspaceResolverTest {

    @Test
    void sanitizeSessionIdAcceptsSafeIds() {
        assertThat(WorkspaceResolver.sanitizeSessionId(null)).isEqualTo("default");
        assertThat(WorkspaceResolver.sanitizeSessionId("  ")).isEqualTo("default");
        assertThat(WorkspaceResolver.sanitizeSessionId("abc-123_x.y")).isEqualTo("abc-123_x.y");
        assertThat(WorkspaceResolver.sanitizeSessionId("sess-1")).isEqualTo("sess-1");
        // Kafka worker ids use ':' as a separator inside a single path segment
        assertThat(WorkspaceResolver.sanitizeSessionId("wf:step:0:1:abcdef12"))
                .isEqualTo("wf:step:0:1:abcdef12");
        // "%2f" is a literal directory name, not a traversal
        assertThat(WorkspaceResolver.sanitizeSessionId("..%2f")).isEqualTo("..%2f");
    }

    @Test
    void sanitizeSessionIdRejectsTraversal() {
        assertThatThrownBy(() -> WorkspaceResolver.sanitizeSessionId(".."))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> WorkspaceResolver.sanitizeSessionId("."))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> WorkspaceResolver.sanitizeSessionId("a/b"))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> WorkspaceResolver.sanitizeSessionId("a\\b"))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> WorkspaceResolver.sanitizeSessionId("/abs"))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> WorkspaceResolver.sanitizeSessionId("a\u0000b"))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> WorkspaceResolver.sanitizeSessionId("a\u0007b"))
                .isInstanceOf(SecurityException.class);
    }

    @Test
    void staticResolveConfinesToSessionRoot() {
        String root = WorkspaceResolver.resolve("sess");
        assertThat(root).endsWith(Path.of("sess").toString());
        assertThat(WorkspaceResolver.resolve("sess", "a/b.txt"))
                .endsWith(Path.of("sess", "a", "b.txt").toString());
    }

    @Test
    void staticResolveRejectsEscapes() {
        assertThatThrownBy(() -> WorkspaceResolver.resolve("sess", "../../etc/passwd"))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> WorkspaceResolver.resolve("sess", "/etc/passwd"))
                .isInstanceOf(SecurityException.class);
        assertThatThrownBy(() -> WorkspaceResolver.resolve("../evil"))
                .isInstanceOf(SecurityException.class);
    }

    @Test
    void sessionDirRejectsTraversal(@TempDir Path base) {
        WorkspaceResolver resolver = new WorkspaceResolver().baseWorkspace(base);
        assertThatThrownBy(() -> resolver.sessionDir("../outside"))
                .isInstanceOf(SecurityException.class);
        assertThat(resolver.sessionDir("sess")).isEqualTo(base.resolve("sess"));
    }

    @Test
    void resolveForReadAllowsSessionFile(@TempDir Path base) throws Exception {
        WorkspaceResolver resolver = new WorkspaceResolver().baseWorkspace(base);
        AgentSessionState state = AgentSessionState.create("sess");
        Files.createDirectories(base.resolve("sess"));
        assertThat(resolver.resolveForRead(state, "file.txt"))
                .isEqualTo(base.resolve("sess").resolve("file.txt"));
    }

    @Test
    void resolveForReadRejectsSymlinkEscape(@TempDir Path base, @TempDir Path outside) throws Exception {
        WorkspaceResolver resolver = new WorkspaceResolver().baseWorkspace(base);
        AgentSessionState state = AgentSessionState.create("sess");
        Files.createDirectories(base.resolve("sess"));
        Path secret = outside.resolve("secret.txt");
        Files.writeString(secret, "top secret");
        Files.createSymbolicLink(base.resolve("sess").resolve("escape"), outside);

        assertThatThrownBy(() -> resolver.resolveForRead(state, "escape/secret.txt"))
                .isInstanceOf(SecurityException.class);
    }

    @Test
    void resolveForWriteRejectsSymlinkEscape(@TempDir Path base, @TempDir Path outside) throws Exception {
        WorkspaceResolver resolver = new WorkspaceResolver().baseWorkspace(base);
        AgentSessionState state = AgentSessionState.create("sess");
        Files.createDirectories(base.resolve("sess"));
        Files.createSymbolicLink(base.resolve("sess").resolve("escape"), outside);

        assertThatThrownBy(() -> resolver.resolveForWrite(state, "escape/new.txt"))
                .isInstanceOf(SecurityException.class);
    }

    @Test
    void realPathResolvesExistingSymlink(@TempDir Path base, @TempDir Path target) throws Exception {
        Path link = base.resolve("link");
        Files.createSymbolicLink(link, target);
        assertThat(WorkspaceResolver.realPath(link)).isEqualTo(target.toRealPath());
    }

    @Test
    void realPathAppendsNonExistingSuffix(@TempDir Path base) throws Exception {
        Files.createDirectories(base.resolve("sub"));
        assertThat(WorkspaceResolver.realPath(base.resolve("sub/new.txt")))
                .isEqualTo(base.resolve("sub").toRealPath().resolve("new.txt"));
    }
}
