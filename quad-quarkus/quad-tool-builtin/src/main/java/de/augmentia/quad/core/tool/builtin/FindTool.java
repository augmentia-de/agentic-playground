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
import org.jboss.logging.Logger;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

@ApplicationScoped
public class FindTool {

    private static final Logger log = Logger.getLogger(FindTool.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int DEFAULT_MAX_RESULTS = 100;

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

    @Tool("Find files by glob pattern (e.g., *.java, src/**/*.ts).")
    public String findFiles(
            @Param("pattern") String pattern,
            @Param(value = "path", required = false) String path,
            @Param(value = "maxResults", required = false) Integer maxResults) {

        var state = currentState();
        if (state == null) {
            return "Error: No active session found";
        }

        if (pattern == null || pattern.isBlank()) {
            return "Error: Glob pattern is required";
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

        String cleanPattern = pattern.replace("\\", "/");
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + cleanPattern);

        // Java glob: a leading "**/" requires a directory boundary, so "**/*.md" never matches
        // files directly in the search root. Test the pattern without that prefix as well.
        PathMatcher topLevelMatcher = cleanPattern.startsWith("**/")
                ? FileSystems.getDefault().getPathMatcher("glob:" + cleanPattern.substring(3))
                : null;

        List<String> matchedFiles = new ArrayList<>();

        try {
            Files.walkFileTree(searchRoot, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs) {
                    if (matchedFiles.size() >= max) {
                        return FileVisitResult.TERMINATE;
                    }
                    try {
                        Path relativePath = searchRoot.relativize(filePath);
                        boolean matches = matcher.matches(relativePath)
                                || (topLevelMatcher != null && topLevelMatcher.matches(relativePath));
                        if (matches) {
                            matchedFiles.add(relativePath.toString().replace("\\", "/"));
                        }
                    } catch (Exception e) {
                        log.warnf(e, "findFiles: skipping %s", filePath);
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    // Report and continue instead of aborting the whole traversal.
                    log.warnf(exc, "findFiles: cannot access %s", file);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            log.warnf(e, "findFiles: traversal error under %s", searchRoot);
            return "Error during file system traversal: " + e.getMessage();
        }

        boolean truncated = matchedFiles.size() >= max;

        ObjectNode json = MAPPER.createObjectNode();
        json.put("pattern", cleanPattern);
        json.put("total", matchedFiles.size());

        var filesArray = json.putArray("files");
        for (String f : matchedFiles) {
            filesArray.add(f);
        }
        json.put("truncated", truncated);

        return json.toPrettyString();
    }
}
