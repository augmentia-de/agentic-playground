package de.augmentia.quad.core.tool.builtin;

import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.CurrentSession;
import de.augmentia.quad.core.session.WorkspaceResolver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class ApplyPatchToolTest {

    @TempDir
    Path tempDir;

    ApplyPatchTool applyPatchTool;

    @BeforeEach
    void setUp() throws Exception {
        applyPatchTool = new ApplyPatchTool() {
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
    void removalUsesContext() throws Exception {
        Files.writeString(tempDir.resolve("test-session/f.txt"), "line1\nline2\nline3\n");
        String patch = "*** Begin Patch\n*** Update File: f.txt\n-line2\n*** End Patch\n";
        String result = applyPatchTool.applyPatch(patch);
        assertThat(result).contains("Updated");
        assertThat(Files.readString(tempDir.resolve("test-session/f.txt"))).isEqualTo("line1\nline3\n");
    }

    @Test
    void addRefusesExisting() throws Exception {
        Files.writeString(tempDir.resolve("test-session/f.txt"), "exists");
        String patch = "*** Begin Patch\n*** Add File: f.txt\n+new\n*** End Patch\n";
        String result = applyPatchTool.applyPatch(patch);
        assertThat(result).containsAnyOf("Error", "exists");
    }

    @Test
    void ioErrorPropagated() throws Exception {
        // Try to update a non-existent path in a way that would fail? Or make file unreadable
        // Simpler: update non-existent file -> should report error, not silently ignore
        String patch = "*** Begin Patch\n*** Update File: missing.txt\n-line\n*** End Patch\n";
        String result = applyPatchTool.applyPatch(patch);
        assertThat(result).contains("Error");
    }
}
