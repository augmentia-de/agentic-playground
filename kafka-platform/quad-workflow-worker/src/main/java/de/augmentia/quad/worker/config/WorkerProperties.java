package de.augmentia.quad.worker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration of the canonical worker.
 *
 * <p>{@link #DEFAULT_MAX_REACT_ITERATIONS} is the fallback for the ReAct limit when an
 * agent definition brings none. It used to be hard-coded to 10 in two places
 * ({@code JdbcAgentConfigSource} and {@code WorkerAgentFactory}) and is now defined in one
 * place and settable via {@code worker.quad.react-max-iterations}.
 */
@ConfigurationProperties(prefix = "worker.quad")
public record WorkerProperties(
    boolean enabled,
    String groupId,
    String taskTopicPattern,
    String resultTopic,
    String dlqTopic,
    int concurrency,
    int maxConcurrentTasks,
    long defaultTimeoutMs,
    String workspaceBase,
    java.util.Map<String, AgentConfig> agents,
    KafkaConfig kafka
) {
    /**
     * Binds {@code kafka} to the defaults when the section is absent.
     *
     * <p>Without this, {@code props.kafka()} is {@code null} for a configuration that has no
     * {@code worker.quad.kafka} block — which is how the shipped {@code application.yml} looks,
     * since every value there can also come from the environment. Every consumer then hits an
     * NPE at bean creation, so the worker refuses to start. Only the live start-up exposed it:
     * unit tests build {@code WorkerProperties} by hand and always passed a KafkaConfig.
     */
    public WorkerProperties {
        if (kafka == null) kafka = new KafkaConfig();
        if (agents == null) agents = java.util.Map.of();
    }
    public record AgentConfig(
        String agentId,
        String modelKey,
        Double temperature,
        Integer maxReActIterations,
        Integer maxOutputTokens,
        java.util.Set<String> allowedTools,
        boolean structuredOutputEnabled,
        String systemPrompt,
        /** Skill directory (classpath or filesystem). Optional. */
        String skillsDir,
        /** Skill names to activate at startup (max 3). Optional. */
        java.util.List<String> initialSkills,
        /** Enable skill_search tool for runtime skill discovery. */
        boolean skillSearchEnabled
    ) {}

    /**
     * @param maxTaskTimeoutMs ceiling for a single task, independent of the workflow's own
     *                        value. Must stay below {@code maxPollIntervalMs}, because the
     *                        listener blocks until the whole batch is done: a task longer than
     *                        that interval evicts the consumer and every task of its partition
     *                        is redelivered, paying for the LLM call twice.
     */
    public record KafkaConfig(
        int maxPollRecords,
        long pollIntervalMs,
        int maxPollIntervalMs,
        long maxTaskTimeoutMs,
        String autoOffsetReset
    ) {
        public KafkaConfig() {
            this(8, 5000, 900000, 600000, "earliest");
        }

        public KafkaConfig {
            // "latest" silently skips a task that was produced before its partition was
            // assigned, which is exactly what happens when a task topic is created at import
            // time. See KafkaConfig for the measurement.
            autoOffsetReset = autoOffsetReset == null || autoOffsetReset.isBlank()
                    ? "earliest" : autoOffsetReset;
        }
    }

    /** Fallback for {@code maxReActIterations}. quad rejects values {@code <= 0}. */
    public static final int DEFAULT_MAX_REACT_ITERATIONS = 10;
}