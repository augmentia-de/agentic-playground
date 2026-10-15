package de.augmentia.quad.core.tool.builtin;

import de.augmentia.quad.core.annotation.Param;
import de.augmentia.quad.core.annotation.Tool;
import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.CurrentSession;
import de.augmentia.quad.core.session.WorkspaceResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Applies a line-based patch (Add / Update / Delete / Move). The applier validates each
 * operation, uses surrounding context to locate removal positions, refuses ADD collisions
 * and propagates I/O errors instead of silently swallowing them.
 */
@ApplicationScoped
public class ApplyPatchTool {

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

    /** Raised for any recoverable patch error; converted into an "Error: ..." result. */
    static final class PatchException extends RuntimeException {
        PatchException(String message) { super(message); }
    }

    @Tool("Apply a patch with Add, Update, Move, Delete operations.")
    public String applyPatch(@Param("patchText") String patchText) {

        var state = currentState();
        if (state == null) {
            return "Error: No active session found";
        }
        if (patchText == null || patchText.isBlank()) {
            return "Error: patchText is required";
        }

        StringBuilder log = new StringBuilder("Patch Results:\n");
        Pending pending = new Pending();
        String[] lines = patchText.split("\\r?\\n", -1);
        int lineCount = lines.length;
        // A trailing newline produces a final empty element that is not a real patch line.
        if (lineCount > 0 && lines[lines.length - 1].isEmpty() && patchText.endsWith("\n")) {
            lines = Arrays.copyOf(lines, lines.length - 1);
        }

        try {
            for (String line : lines) {
                if (line.startsWith("*** Begin Patch") || line.startsWith("*** End Patch")) {
                    continue;
                }
                if (line.startsWith("*** Add File:")) {
                    flush(state, pending, log);
                    pending.reset(resolveWrite(state, line.substring("*** Add File:".length()).trim()), "ADD");
                } else if (line.startsWith("*** Update File:")) {
                    flush(state, pending, log);
                    pending.reset(resolveWrite(state, line.substring("*** Update File:".length()).trim()), "UPDATE");
                } else if (line.startsWith("*** Delete File:")) {
                    flush(state, pending, log);
                    Path delFile = resolveWrite(state, line.substring("*** Delete File:".length()).trim());
                    try {
                        if (Files.deleteIfExists(delFile)) {
                            log.append("Deleted: ").append(delFile.getFileName()).append("\n");
                        }
                    } catch (java.io.IOException e) {
                        throw new PatchException(e.getMessage());
                    }
                } else if (line.startsWith("*** Move to:")) {
                    if (!"UPDATE".equals(pending.mode) || pending.file == null) {
                        throw new PatchException("Move to without a preceding Update File");
                    }
                    pending.moveTarget = resolveWrite(state, line.substring("*** Move to:".length()).trim());
                } else {
                    if (pending.mode == null) {
                        if (line.isBlank()) continue;
                        throw new PatchException("unexpected content outside of a file section: " + line);
                    }
                    pending.body.add(line);
                }
            }
            flush(state, pending, log);
        } catch (SecurityException e) {
            return "Access denied: " + e.getMessage();
        } catch (PatchException e) {
            return "Error: " + e.getMessage();
        }

        return log.toString();
    }

    private Path resolveWrite(AgentSessionState state, String subPath) {
        if (subPath == null || subPath.isBlank()) {
            throw new PatchException("file path is required");
        }
        return getWorkspaceResolver().resolveForWrite(state, subPath);
    }

    private void flush(AgentSessionState state, Pending pending, StringBuilder log) {
        if (pending.mode == null || pending.file == null) {
            pending.reset(null, null);
            return;
        }
        try {
            if ("ADD".equals(pending.mode)) {
                if (Files.exists(pending.file)) {
                    throw new PatchException("cannot add file, it already exists: " + pending.file.getFileName());
                }
                String content = bodyToString(pending.body, true);
                createParent(pending.file);
                Files.writeString(pending.file, content);
                log.append("Added: ").append(pending.file.getFileName()).append("\n");
            } else if ("UPDATE".equals(pending.mode)) {
                if (!Files.exists(pending.file)) {
                    throw new PatchException("cannot update file, it does not exist: " + pending.file.getFileName());
                }
                String original = Files.readString(pending.file);
                String updated = applyBody(original, pending.body);
                Files.writeString(pending.file, updated);
                log.append("Updated: ").append(pending.file.getFileName()).append("\n");
            }
            if (pending.moveTarget != null) {
                createParent(pending.moveTarget);
                Files.move(pending.file, pending.moveTarget, StandardCopyOption.REPLACE_EXISTING);
                log.append("Moved to: ").append(pending.moveTarget.getFileName()).append("\n");
            }
        } catch (PatchException e) {
            throw e;
        } catch (Exception e) {
            throw new PatchException(e.getMessage());
        } finally {
            pending.reset(null, null);
        }
    }

    private static void createParent(Path file) throws java.io.IOException {
        Path parent = file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
    }

    private static String bodyToString(List<String> body, boolean trailingNewline) {
        StringBuilder sb = new StringBuilder();
        for (String line : body) {
            if (line.startsWith("+")) {
                sb.append(line.substring(1)).append("\n");
            } else if (line.isEmpty()) {
                sb.append("\n");
            } else {
                throw new PatchException("ADD section may only contain '+' lines: " + line);
            }
        }
        String result = sb.toString();
        if (trailingNewline && result.isEmpty()) {
            return "";
        }
        return result;
    }

    private static String applyBody(String original, List<String> body) {
        boolean trailing = original.endsWith("\n");
        List<String> lines = new ArrayList<>(Arrays.asList(original.split("\n", -1)));
        if (trailing && !lines.isEmpty()) {
            lines.remove(lines.size() - 1);
        }

        List<String> result = new ArrayList<>();
        int cursor = 0;
        for (String bodyLine : body) {
            char prefix = bodyLine.isEmpty() ? ' ' : bodyLine.charAt(0);
            if (prefix == '+') {
                result.add(bodyLine.substring(1));
            } else if (prefix == '-' || prefix == ' ') {
                String content = bodyLine.isEmpty() ? "" : bodyLine.substring(1);
                int idx = indexOf(lines, content, cursor);
                if (idx < 0) {
                    throw new PatchException("could not locate line: " + content);
                }
                for (int k = cursor; k < idx; k++) {
                    result.add(lines.get(k));
                }
                if (prefix == ' ') {
                    result.add(lines.get(idx));
                }
                cursor = idx + 1;
            } else {
                // Bare context line without a leading space.
                int idx = indexOf(lines, bodyLine, cursor);
                if (idx < 0) {
                    throw new PatchException("could not locate context line: " + bodyLine);
                }
                for (int k = cursor; k <= idx; k++) {
                    result.add(lines.get(k));
                }
                cursor = idx + 1;
            }
        }
        for (int k = cursor; k < lines.size(); k++) {
            result.add(lines.get(k));
        }

        String joined = String.join("\n", result);
        return trailing ? joined + "\n" : joined;
    }

    private static int indexOf(List<String> lines, String content, int from) {
        for (int i = Math.max(0, from); i < lines.size(); i++) {
            if (lines.get(i).equals(content)) {
                return i;
            }
        }
        return -1;
    }

    private static final class Pending {
        Path file;
        String mode;
        Path moveTarget;
        List<String> body = new ArrayList<>();

        void reset(Path file, String mode) {
            this.file = file;
            this.mode = mode;
            this.moveTarget = null;
            this.body = new ArrayList<>();
        }
    }
}
