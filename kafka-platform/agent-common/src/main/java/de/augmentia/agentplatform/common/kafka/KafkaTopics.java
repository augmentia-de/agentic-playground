package de.augmentia.agentplatform.common.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class KafkaTopics {

    public static final String WorkflowKey = "workflow_id";
    public static final String TaskKey = "task_id";
    public static final String SweepKey = "sweep_id";
    public static final String AgentTypeKey = "agent_type";

    public static final TopicSpec INTAKE = delete(TopicNames.INTAKE, 12, WorkflowKey,
            Duration.ofDays(7), "Incoming workflow trigger");

    public static final TopicSpec RESULTS = delete(TopicNames.RESULTS, 12, WorkflowKey,
            Duration.ofDays(7), "Execution results & status");

    public static final TopicSpec TIMEOUTS = delete(TopicNames.TIMEOUTS, 6, TaskKey,
            Duration.ofDays(3), "Delayed timeout events");

    public static final TopicSpec CANCELLATION = delete(TopicNames.CANCELLATION, 6, WorkflowKey,
            Duration.ofDays(1), "Cancellation signals");

    public static final TopicSpec HITL = delete(TopicNames.HITL, 6, WorkflowKey,
            Duration.ofDays(14), "Human review escalation");

    public static final TopicSpec DLQ = delete(TopicNames.DLQ, 6, WorkflowKey,
            Duration.ofDays(30), "Dead letter queue");

    public static final TopicSpec LOOP = delete(TopicNames.LOOP, 6, WorkflowKey,
            Duration.ofDays(3), "Loop iteration advancement");

    public static final TopicSpec SWEEP = delete(TopicNames.SWEEP, 1, SweepKey,
            Duration.ofDays(1), "Routing sweep / reaper ticks");

    public static final TopicSpec CONFIG_UPDATES = compact(TopicNames.CONFIG_UPDATES, 1, AgentTypeKey,
            "Dynamic config broadcast");

    private KafkaTopics() {}

    public static TopicSpec tasks(String agentType) {
        if (agentType == null || agentType.isBlank()) {
            throw new IllegalArgumentException("agentType required");
        }
        return delete(TopicNames.tasks(agentType), 12, WorkflowKey, Duration.ofDays(7),
                "Task queue for agent type '" + agentType + "'");
    }

    public static List<TopicSpec> all(Collection<String> agentTypes) {
        List<TopicSpec> specs = new ArrayList<>(List.of(
                INTAKE, RESULTS, TIMEOUTS, CANCELLATION, HITL, DLQ, LOOP, SWEEP, CONFIG_UPDATES));
        agentTypes.stream()
                .filter(agentType -> agentType != null && !agentType.isBlank())
                .map(String::toLowerCase)
                .distinct()
                .sorted()
                .map(KafkaTopics::tasks)
                .forEach(specs::add);
        return List.copyOf(specs);
    }

    private static TopicSpec delete(String name, int partitions, String keyStrategy, Duration retention, String description) {
        return new TopicSpec(name, partitions, false, retention, keyStrategy, description);
    }

    private static TopicSpec compact(String name, int partitions, String keyStrategy, String description) {
        return new TopicSpec(name, partitions, true, null, keyStrategy, description);
    }
}
