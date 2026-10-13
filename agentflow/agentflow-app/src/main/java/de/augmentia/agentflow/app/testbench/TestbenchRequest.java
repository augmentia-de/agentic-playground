package de.augmentia.agentflow.app.testbench;

import java.util.List;

/** Request for the agent testbench. */
public record TestbenchRequest(String systemPrompt, String prompt, List<String> tools) {
}
