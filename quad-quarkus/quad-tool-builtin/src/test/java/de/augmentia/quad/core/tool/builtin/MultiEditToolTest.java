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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MultiEditToolTest {

    @TempDir
    Path tempDir;

    MultiEditTool multiEditTool;

    @BeforeEach
    void setUp() throws Exception {
        multiEditTool = new MultiEditTool() {
            @Override
            protected WorkspaceResolver getWorkspaceResolver() {
                return new WorkspaceResolver() {
                    @Override
                    public Path workspaceDir() {
                        return tempDir;
                    }

                    @Override
                    public Path resolveForRead(AgentSessionState state, String path) {
                        Path resolved = tempDir.resolve(state.getSessionId()).resolve(path).normalize();
                        if (!resolved.startsWith(tempDir.resolve(state.getSessionId()))) {
                            throw new SecurityException("Access denied");
                        }
                        return resolved;
                    }

                    @Override
                    public Path resolveForWrite(AgentSessionState state, String path) {
                        Path resolved = tempDir.resolve(state.getSessionId()).resolve(path).normalize();
                        if (!resolved.startsWith(tempDir.resolve(state.getSessionId()))) {
                            throw new SecurityException("Access denied");
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
    }

    @Test
    void failure_leavesNoPartialEdits() throws IOException {
        Files.writeString(tempDir.resolve("test-session/f.txt"), "hello world");
        MultiEditTool.EditOperation ok = new MultiEditTool.EditOperation("f.txt", "hello", "hi", false);
        MultiEditTool.EditOperation bad = new MultiEditTool.EditOperation("f.txt", "NOTFOUND", "x", false);
        String result = multiEditTool.multiEdit(List.of(ok, bad));
        // Failure means partial state remains - WP09 expects atomicity (no partial changes on failure)
        // After failure, file should NOT be modified
        assertThat(result).startsWith("Error:"); // current impl returns Error early
        assertThat(Files.readString(tempDir.resolve("test-session/f.txt"))).isEqualTo("hello world");
    }
}
