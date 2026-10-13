package de.augmentia.agentplatform.orchestrator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "app")
public record OrchestratorProperties(
        Kafka kafka,
        Orchestration orchestration,
        Hitl hitl,
        Seeding seeding,
        AutoScaling autoScaling,
        Vault vault,
        Lease lease,
        Agents agents
) {

    public OrchestratorProperties {
        kafka = kafka == null ? new Kafka(List.of()) : kafka;
        orchestration = orchestration == null
                ? new Orchestration(60_000L, 3, 5_000L, 1_000L, 600_000L)
                : orchestration;
        hitl = hitl == null ? new Hitl(24L, 3) : hitl;
        seeding = seeding == null ? new Seeding(true, "classpath*:workflows/*.json") : seeding;
        autoScaling = autoScaling == null ? new AutoScaling(true, 30, 500, 2, 20, null, null) : autoScaling;
        vault = vault == null ? new Vault(true, "http://vault:8200", "", "secret/data", 60L) : vault;
        lease = lease == null ? new Lease("orchestrator-1", 30L) : lease;
        agents = agents == null ? new Agents("simple") : agents;
    }

    /**
     * @param defaultModelKey model key written into an auto-registered stub agent. It must
     *                         match a key the worker actually has registered; the worker
     *                         resolves agents with {@code models.get(modelKey)} and throws on
     *                         anything unknown.
     */
    // No convenience constructor for the older arity on purpose: this record is bound by
    // @ConfigurationProperties, and a second constructor makes the binder look for a no-arg
    // constructor, fail to find one and abort the whole context at start-up.
    public record Agents(String defaultModelKey) {
        public Agents {
            defaultModelKey = defaultModelKey == null || defaultModelKey.isBlank()
                    ? "simple" : defaultModelKey;
        }
    }

    public record Kafka(List<String> agentTypes) {
        public Kafka {
            agentTypes = agentTypes == null ? List.of() : List.copyOf(agentTypes);
        }
    }

    public record Orchestration(
            long defaultTimeoutMs,
            int defaultMaxAttempts,
            long sweepIntervalMs,
            long sweepStaleGraceMs,
            long maxTaskTimeoutMs
    ) {
        public Orchestration {
            sweepIntervalMs = sweepIntervalMs <= 0 ? 5_000L : sweepIntervalMs;
            sweepStaleGraceMs = sweepStaleGraceMs <= 0 ? 1_000L : sweepStaleGraceMs;
            // Cap for the task timeout. The worker blocks its poll thread until the whole
            // batch is done; a timeout beyond that limit leads to a rebalance and thus to
            // duplicate LLM calls. The value must be smaller than
            // worker.quad.kafka.max-poll-interval-ms (default 900000).
            maxTaskTimeoutMs = maxTaskTimeoutMs <= 0 ? 600_000L : maxTaskTimeoutMs;
        }
    }

    public record Hitl(long ttlHours, int maxEscalations) {}

    public record Seeding(boolean enabled, String locations) {}

    /**
     * Auto-scaling thresholds and the topic whose lag is measured.
     *
     * <p>{@code lagTopic}/{@code lagGroupId} are deliberately configurable rather than
     * hard-coded. The measured lag must belong to the consumer group that actually reads
     * the topic — otherwise you scale on a signal that has nothing to do with the workload.
     * The default deliberately points at {@code agent.control.loop} /
     * {@code orchestrator-dag-group}, that is, at the orchestrator itself (see
     * {@code DagTopicListener}). For real worker scaling both values have to be changed,
     * for example to an {@code agent.tasks.*} topic with the worker's group.
     */
    public record AutoScaling(
            boolean enabled,
            int pollIntervalSeconds,
            int lagThreshold,
            int minWorkers,
            int maxWorkers,
            String lagTopic,
            String lagGroupId
    ) {
        public static final String DEFAULT_LAG_GROUP_ID = "orchestrator-dag-group";

        public AutoScaling {
            lagTopic = (lagTopic == null || lagTopic.isBlank())
                    ? de.augmentia.agentplatform.common.kafka.TopicNames.LOOP : lagTopic;
            lagGroupId = (lagGroupId == null || lagGroupId.isBlank())
                    ? DEFAULT_LAG_GROUP_ID : lagGroupId;
        }
    }

    public record Vault(
            boolean enabled,
            String url,
            String token,
            String pathPrefix,
            long cacheTtlMinutes
    ) {}

    /** Lock lease configuration — only one orchestrator instance holds the lock at a time */
    public record Lease(String instanceId, long lockExpirationMinutes) {
        public Lease {
            instanceId = instanceId == null ? "unknown" : instanceId;
            lockExpirationMinutes = lockExpirationMinutes <= 0 ? 30L : lockExpirationMinutes;
        }
    }
}
