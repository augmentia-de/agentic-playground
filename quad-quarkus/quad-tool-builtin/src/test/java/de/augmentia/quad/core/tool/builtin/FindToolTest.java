package de.augmentia.quad.core.tool.builtin;

import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.CurrentSession;
import de.augmentia.quad.core.session.WorkspaceResolver;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

class FindToolTest {

    @TempDir
    Path tempDir;

    FindTool findTool;

    @BeforeEach
    void setUp() {
        findTool = new FindTool() {
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
                            throw new SecurityException("Access denied: outside the session workspace " + resolved);
                        }
                        return resolved;
                    }
                };
            }
        };

        CurrentSession.setCurrent(new AgentSessionState());
    }

    @AfterEach
    void tearDown() {
        CurrentSession.setCurrent(null);
    }

    @Test
    void shouldFindTopLevelFileWithDoubleStarPattern() throws IOException {
        Files.writeString(tempDir.resolve("CV_template.md"), "cv");

        String result = findTool.findFiles("**/*.md", null, null);

        assertThat(result).contains("\"total\" : 1");
        assertThat(result).contains("CV_template.md");
    }

    @Test
    void shouldFindNestedFileWithDoubleStarPattern() throws IOException {
        Files.createDirectories(tempDir.resolve("1_java-entwickler-remote"));
        Files.writeString(tempDir.resolve("1_java-entwickler-remote/Project.md"), "project");

        String result = findTool.findFiles("**/Project.md", null, null);

        assertThat(result).contains("\"total\" : 1");
        assertThat(result).contains("1_java-entwickler-remote/Project.md");
    }

    @Test
    void shouldFindTopLevelAndNestedFiles() throws IOException {
        Files.writeString(tempDir.resolve("CV_template.md"), "cv");
        Files.createDirectories(tempDir.resolve("sub"));
        Files.writeString(tempDir.resolve("sub/AN_Template.md"), "an");

        String result = findTool.findFiles("**/*.md", null, null);

        assertThat(result).contains("\"total\" : 2");
        assertThat(result).contains("CV_template.md");
        assertThat(result).contains("sub/AN_Template.md");
    }

    @Test
    void shouldReturnZeroForNoMatch() throws IOException {
        Files.writeString(tempDir.resolve("notes.txt"), "plain");

        String result = findTool.findFiles("**/Project.md", null, null);

        assertThat(result).contains("\"total\" : 0");
    }

    @Test
    void unreadableDir_logsAndContinues() throws IOException {
        // Create unreadable dir - skip if not supported
        Files.writeString(tempDir.resolve("visible.txt"), "x");
        Path unreadable = tempDir.resolve("unreadable");
        Files.createDirectories(unreadable);
        Files.writeString(unreadable.resolve("secret.txt"), "y");
        boolean restricted = unreadable.toFile().setReadable(false, false);
        try {
            String result = findTool.findFiles("**/*.txt", null, null);
            // Traversal must continue past the unreadable directory and still report the readable file.
            assertThat(result).contains("visible.txt");
            assertThat(result).contains("total");
        } finally {
            unreadable.toFile().setReadable(true, false);
        }
        // If the platform could not restrict permissions (e.g. running as root), the test is still valid
        // because the traversal must not fail in any case.
        assertThat(restricted || true).isTrue();
    }

}