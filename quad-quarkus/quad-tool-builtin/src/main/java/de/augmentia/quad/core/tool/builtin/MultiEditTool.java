package de.augmentia.quad.core.tool.builtin;

import de.augmentia.quad.core.annotation.Param;
import de.augmentia.quad.core.annotation.Tool;
import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.CurrentSession;
import de.augmentia.quad.core.session.WorkspaceResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Applies multiple string replacements across one or more files with all-or-nothing semantics:
 * every edit is validated first and written only afterwards. If a write fails, already written
 * files are rolled back to their original content.
 */
@ApplicationScoped
public class MultiEditTool {

    private static final Logger log = Logger.getLogger(MultiEditTool.class);

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

    @Tool("Perform multiple string replacements across one or more files atomically. All edits are validated before any file is written.")
    public String multiEdit(@Param("edits") List<EditOperation> edits) {

        var state = currentState();
        if (state == null) {
            return "Error: No active session found";
        }

        if (edits == null || edits.isEmpty()) {
            return "Error: No edits provided";
        }

        // Phase 1: resolve and validate every edit, staging the new content per file.
        Map<Path, String> staged = new LinkedHashMap<>();
        for (EditOperation edit : edits) {
            Path targetPath;
            try {
                targetPath = getWorkspaceResolver().resolveForWrite(state, edit.filePath());
            } catch (SecurityException e) {
                return "Access denied: " + e.getMessage();
            }

            if (!Files.exists(targetPath)) {
                return "Error: File does not exist: " + edit.filePath();
            }

            String content = staged.get(targetPath);
            if (content == null) {
                try {
                    content = Files.readString(targetPath);
                } catch (Exception e) {
                    return "Error: Could not read file: " + edit.filePath() + " (" + e.getMessage() + ")";
                }
            }

            if (edit.oldString() == null || !content.contains(edit.oldString())) {
                return "Error: Old string not found in file: " + edit.filePath();
            }

            String newContent;
            if (Boolean.TRUE.equals(edit.replaceAll())) {
                newContent = content.replace(edit.oldString(), edit.newString());
            } else {
                int firstIdx = content.indexOf(edit.oldString());
                int lastIdx = content.lastIndexOf(edit.oldString());
                if (firstIdx != lastIdx) {
                    return "Error: Old string is not unique in file: " + edit.filePath()
                            + " (use replaceAll=true to replace all occurrences)";
                }
                newContent = content.substring(0, firstIdx) + edit.newString()
                        + content.substring(firstIdx + edit.oldString().length());
            }
            staged.put(targetPath, newContent);
        }

        // Phase 2: snapshot originals, then write all staged files; roll back on any failure.
        Map<Path, String> originals = new LinkedHashMap<>();
        List<Path> written = new ArrayList<>();
        try {
            for (Path p : staged.keySet()) {
                originals.put(p, Files.readString(p));
            }
            for (Map.Entry<Path, String> e : staged.entrySet()) {
                Files.writeString(e.getKey(), e.getValue());
                written.add(e.getKey());
            }
        } catch (Exception e) {
            for (Path p : written) {
                try {
                    Files.writeString(p, originals.get(p));
                } catch (Exception rollbackErr) {
                    log.errorf(rollbackErr, "multiEdit: rollback failed for %s", p);
                }
            }
            return "Error: " + e.getMessage();
        }

        StringBuilder log2 = new StringBuilder("Execution summary:\n");
        for (Path p : staged.keySet()) {
            log2.append("Edited: ").append(p.getFileName()).append("\n");
        }
        return log2.toString();
    }

    public record EditOperation(String filePath, String oldString, String newString, Boolean replaceAll) {
    }
}
