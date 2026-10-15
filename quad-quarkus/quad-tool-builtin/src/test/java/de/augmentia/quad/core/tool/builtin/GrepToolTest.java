package de.augmentia.quad.core.tool.builtin;

import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.CurrentSession;
import de.augmentia.quad.core.session.WorkspaceResolver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class GrepToolTest {

    @TempDir
    Path tempDir;

    GrepTool grepTool;

    @BeforeEach
    void setUp() throws Exception {
        grepTool = new GrepTool() {
            @Override
            protected WorkspaceResolver getWorkspaceResolver() {
                return new WorkspaceResolver() {
                    @Override
                    public Path workspaceDir() {
                        return tempDir;
                    }

                    @Override
                    public Path resolveForRead(AgentSessionState state, String path) {
                        Path resolved = tempDir.resolve(path).normalize();
                        if (!resolved.startsWith(tempDir)) {
                            throw new SecurityException("Access denied: outside the session workspace");
                        }
                        return resolved;
                    }

                    @Override
                    public Path sessionDir(String sessionId) {
                        return tempDir.resolve(sessionId);
                    }
                };
            }
        };
        AgentSessionState testSessionState = new AgentSessionState();
        Field f = AgentSessionState.class.getDeclaredField("sessionId");
        f.setAccessible(true);
        f.set(testSessionState, "test-session");
        CurrentSession.setCurrent(testSessionState);
        Files.createDirectories(tempDir.resolve("test-session"));
    }

    @AfterEach
    void tearDown() {
        CurrentSession.setCurrent(null);
        System.clearProperty("quad.grep.timeoutMs");
    }

    @Test
    void catastrophicPattern_timesOut() throws IOException {
        // Aggressive-but-positive timeout so the test stays fast; exponential backtracking
        // must be aborted by the deadline guard.
        System.setProperty("quad.grep.timeoutMs", "300");
        Files.writeString(tempDir.resolve("test-session/evil.txt"), "a".repeat(40) + "!");

        long start = System.nanoTime();
        String result = grepTool.grepSearch("^\\s*(([a-z])\\2*)+$", tempDir.resolve("test-session").toString(), null, false, null);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(result).contains("timeout");
        assertThat(result).doesNotContain("No matches found");
        assertThat(elapsedMs).isLessThan(30_000L);
    }

    @Test
    void binaryFiles_skipped() throws IOException {
        // .class file with a NUL byte
        Files.write(tempDir.resolve("test-session/test.class"), new byte[]{(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE, 0, 1, 2, 3});
        Files.writeString(tempDir.resolve("test-session/data.txt"), "hello\n");
        String result = grepTool.grepSearch("hello", tempDir.resolve("test-session").toString(), null, false, 100);
        assertThat(result).contains("data.txt");
        assertThat(result).doesNotContain("test.class");
    }

    @Test
    void includeGlob_escapesRegexMeta() throws IOException {
        Files.writeString(tempDir.resolve("test-session/xtxt"), "needle\n");
        Files.writeString(tempDir.resolve("test-session/a.txt"), "needle\n");
        String result = grepTool.grepSearch("needle", tempDir.resolve("test-session").toString(), "*.txt", false, 100);
        assertThat(result).contains("a.txt");
        assertThat(result).doesNotContain("xtxt");
    }
}
