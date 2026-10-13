package de.augmentia.agentflow.app.tools;

import de.augmentia.quad.core.annotation.Tool;

/** Tiny tool used to demonstrate provenance tracking of tool executions. */
public class ClockTool {

    @Tool("Returns the current server time in ISO-8601")
    public String now() {
        return java.time.Instant.now().toString();
    }
}
