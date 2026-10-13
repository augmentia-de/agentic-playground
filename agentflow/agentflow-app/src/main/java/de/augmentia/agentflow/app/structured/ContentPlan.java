package de.augmentia.agentflow.app.structured;

import java.util.List;

/** Example typed output for structured agents (LangChain4j generates the JSON schema). */
public record ContentPlan(String topic, List<String> sections, String audience) {
}
