package de.augmentia.agentplatform.common.kafka;

public final class TopicNames {

    public static final String INTAKE = "agent.execution.intake";
    public static final String RESULTS = "agent.execution.results";
    public static final String DLQ = "agent.execution.dlq";
    public static final String TIMEOUTS = "agent.control.timeouts";
    public static final String CANCELLATION = "agent.control.cancellation";
    public static final String HITL = "agent.control.hitl";
    public static final String LOOP = "agent.control.loop";
    public static final String SWEEP = "agent.control.sweep";
    public static final String CONFIG_UPDATES = "agent.config.updates";
    public static final String TASKS_PREFIX = "agent.tasks.";

    private TopicNames() {}

    /**
     * Task topic for an agent type (non-tenant-aware, legacy).
     */
    public static String tasks(String agentType) {
        return TASKS_PREFIX + agentType.toLowerCase();
    }

    /**
     * Task topic with tenant partitioning. Returns "agent.tasks.{agent_type}.{tenant}".
     */
    public static String tasks(String agentType, String tenant) {
        if (tenant == null || tenant.isBlank()) {
            return tasks(agentType);
        }
        return TASKS_PREFIX + agentType.toLowerCase() + "." + tenant.toLowerCase();
    }
}
