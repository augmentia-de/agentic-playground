package de.augmentia.quad.core.tool.sandbox;

import java.util.ArrayList;
import java.util.List;

/**
 * Thread-safe, bounded line buffer for sandbox output.
 *
 * <p>Both the stdout and the stderr pump of a sandbox process feed into the same buffer.
 * The oldest lines are dropped once either the line count or the byte budget is exceeded,
 * so a command that floods a stream (e.g. {@code yes}) can no longer grow the retained
 * output without bound and exhaust the heap (B30). Previously only stdout was bounded.
 */
final class BoundedOutput {

    /** Maximum number of retained lines. */
    static final int MAX_LINES = 300;
    /** Soft byte budget per stream; trimming starts above twice this value. */
    static final int MAX_BYTES = 30_720;

    private final List<String> lines = new ArrayList<>();
    private long bytes;

    /** Appends one line, then trims the buffer back inside its bounds. */
    synchronized void add(String line) {
        if (line == null) {
            return;
        }
        lines.add(line);
        bytes += line.length() + 1;
        while (lines.size() > MAX_LINES || bytes > (long) MAX_BYTES * 2) {
            String removed = lines.remove(0);
            bytes -= removed.length() + 1;
        }
    }

    /** Snapshot of the retained lines, oldest first. */
    synchronized List<String> snapshot() {
        return List.copyOf(lines);
    }

    /** The retained lines joined by {@code \n}. */
    synchronized String join() {
        return String.join("\n", lines);
    }

    /** Number of currently retained lines. */
    synchronized int lineCount() {
        return lines.size();
    }

    /** Number of currently retained bytes (line length + separator). */
    synchronized long byteCount() {
        return bytes;
    }
}
