package de.augmentia.agentplatform.common.kafka;

import java.time.Duration;

/**
 * Descriptor for one Kafka topic (SPEC2 §4.1).
 *
 * @param partitions  replication requested partition count
 * @param compacted   true → cleanup.policy=compact (config topic), false → delete
 * @param retention   retention for delete topics; null for compacted topics
 * @param keyStrategy partitioning key strategy name (workflow_id / task_id / sweep_id / agent_type)
 * @param description human-readable purpose
 */
public record TopicSpec(
        String name,
        int partitions,
        boolean compacted,
        Duration retention,
        String keyStrategy,
        String description
) {

    public Duration retentionOr(Duration defaultRetention) {
        return retention != null ? retention : defaultRetention;
    }
}