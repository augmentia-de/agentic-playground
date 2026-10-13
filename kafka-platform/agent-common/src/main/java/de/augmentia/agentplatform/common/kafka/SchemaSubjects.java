package de.augmentia.agentplatform.common.kafka;

/**
 * Confluent Schema Registry subject names (SPEC2 §4.2). JSON Schema, BACKWARD.
 */
public final class SchemaSubjects {

    private SchemaSubjects() {}

    public static String tasks(String agentType) {
        return "agent.tasks." + agentType + "-value";
    }

    public static final String RESULTS = "agent.execution.results-value";
    public static final String TIMEOUTS = "agent.control.timeouts-value";
    public static final String HITL = "agent.control.hitl-value";
    public static final String LOOP = "agent.control.loop-value";
    public static final String SWEEP = "agent.control.sweep-value";
    public static final String CONFIG_UPDATES = "agent.config.updates-value";
}