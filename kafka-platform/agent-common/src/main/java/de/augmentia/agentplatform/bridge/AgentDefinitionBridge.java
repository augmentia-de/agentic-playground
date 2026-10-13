package de.augmentia.agentplatform.bridge;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Converts Quarkus-rich {@code AgentDefinition} objects into the reduced
 * Kafka-Worker {@link WorkerAgentConfig} format.
 *
 * <p>Features lost in the conversion:
 * <ul>
 *   <li><b>Skills</b> — skill registry integration, dynamic loading</li>
 *   <li><b>Named hooks</b> — pre/post agent/tool call hooks</li>
 *   <li><b>Permissions</b> — granted directories (read/write levels)</li>
 *   <li><b>MCP/Sandbox</b> — MCP tool client, Docker sandbox support</li>
 * </ul>
 *
 * <p>The mapping preserves what the Kafka orchestrator supports:
 * <table border="1" summary="Field mapping">
 *   <tr><th>Quarkus Field</th><th>Kafka Target</th><th>Mapping</th></tr>
 *   <tr><td>{@code id}</td><td>{@code agentId}</td><td>direct</td></tr>
 *   <tr><td>{@code modelId}</td><td>{@code modelKey}</td><td>direct / resolve alias</td></tr>
 *   <tr><td>{@code temperature}</td><td>{@code temperature}</td><td>direct</td></tr>
 *   <tr><td>{@code maxTokens}</td><td>{@code maxOutputTokens}</td><td>direct (Default 1024)</td></tr>
 *   <tr><td>{@code systemPrompt}</td><td>{@code systemPrompt}</td><td>direct</td></tr>
 *   <tr><td>{@code toolDefinitions}</td><td>{@code allowedTools}</td><td>names only (no params/perms)</td></tr>
 * </table>
 *
 * <p><b>No iteration count.</b> Until recently the record contained a field
 * {@code maxReActIterations} which in truth was a token budget: the value lands unchanged in
 * {@code agent_definitions.max_tokens} and, in the worker, in
 * {@code paramsBuilder.maxOutputTokens(...)}. The worker's iteration limit comes from
 * {@code JdbcAgentConfigSource} and is fixed at 10 there.
 */
public class AgentDefinitionBridge {

    /**
     * Convert a Quarkus AgentDefinition into a Kafka WorkerAgentConfig.
     *
     * @param quadAgent rich definition from Quarkus backend
     * @return reduced config suitable for Kafka worker consumption
     */
    public static WorkerAgentConfig fromQuadAgent(QuarkusAgent quadAgent) {
        return fromQuadAgent(quadAgent, DEFAULT_MAX_REACT_ITERATIONS);
    }

    /**
     * Convert a Quarkus AgentDefinition into a Kafka WorkerAgentConfig.
     *
     * @param quadAgent rich definition from Quarkus backend
     * @param defaultMaxReActIterations fallback when quad sends no iteration limit.
     *        Must be {@code > 0} — quad itself rejects anything else in
     *        {@code Agent.setReActMaxIterations}. Keeping it a parameter is what makes
     *        the fallback configurable: this class has no configuration of its own, so the
     *        caller owns the value.
     * @return reduced config suitable for Kafka worker consumption
     */
    public static WorkerAgentConfig fromQuadAgent(QuarkusAgent quadAgent, int defaultMaxReActIterations) {
        String agentId = quadAgent.id() != null ? quadAgent.id() : quadAgent.name();
        Double temperature = quadAgent.temperature();
        Integer maxTokens = quadAgent.maxTokens();
        Integer timeoutSeconds = quadAgent.timeoutSeconds();
        Integer maxRetryAttempts = quadAgent.maxRetryAttempts();
        Set<String> tools = extractToolNames(quadAgent.toolDefinitions());
        
        return new WorkerAgentConfig(
                agentId,
                quadAgent.name(),
                quadAgent.category(),
                resolveModelKey(quadAgent.modelId()),
                temperature != null ? temperature : 0.20,
                maxOutputTokensFrom(maxTokens),
                tools,
                quadAgent.systemPrompt(),
                timeoutSeconds != null ? timeoutSeconds : 120,
                maxRetryAttempts != null ? maxRetryAttempts : 3,
                quadAgent.version(),
                maxReActIterationsFrom(quadAgent.maxReActIterations(), defaultMaxReActIterations)
        );
    }

    /**
     * Bulk convert multiple agents.
     */
    public static List<WorkerAgentConfig> fromQuadAgents(List<QuarkusAgent> agents) {
        return agents.stream()
                .map(AgentDefinitionBridge::fromQuadAgent)
                .toList();
    }

    // ── Private helpers ────────────────────────────────────────────────

    /**
     * Resolve Quarkus {@code modelId} to Kafka {@code modelKey}.
     * Simple identity resolution; can be extended with an alias map.
     *
     * Example: "gpt-4o" → "gpt-4o", "anthropic-claude-3" → "claude-3"
     */
    private static String resolveModelKey(String modelId) {
        if (modelId == null || modelId.isBlank()) {
            return "gpt-4o";
        }
        // Normalize known aliases
        return switch (modelId.toLowerCase()) {
            case "claude-3-opus" -> "claude-3";
            case "claude-3-sonnet" -> "claude-3";
            case "claude-3-haiku" -> "claude-3";
            case "gemini-pro" -> "gemini";
            default -> modelId;
        };
    }

    /**
     * Upper bound for {@code maxTokens}.
     *
     * <p>The value travels to {@code agent_definitions.max_tokens} and from the worker to
     * {@code paramsBuilder.maxOutputTokens(...)} — so it is a token budget for the answer,
     * not an iteration count. Both were conflated here: the field was called
     * {@code maxReActIterations} and was routed via {@code maxTokens / 4}, which the caller
     * then reverse-calculated with {@code * 4}. The two calculations cancelled out, but the
     * default did not: without {@code maxTokens} the value 8 remained, 32 after reverse
     * calculation — the worker would have truncated every answer to 32 tokens.
     *
     * <p>The default is therefore a token budget of the same magnitude that
     * {@code JdbcAgentDefinitionStore.ensureStub} writes for the same column.
     */
    private static final int DEFAULT_MAX_OUTPUT_TOKENS = 1024;

    /**
     * Fallback for the ReAct iteration limit when neither quad nor the caller supplies a
     * value. Matches what the worker previously hard-coded to 10.
     */
    public static final int DEFAULT_MAX_REACT_ITERATIONS = 10;

    /**
     * The value is passed through. quad is the source for the token budget; a conversion
     * here would be a second interpretation of the same number in two places.
     */
    private static int maxOutputTokensFrom(Integer maxTokens) {
        if (maxTokens == null || maxTokens <= 0) return DEFAULT_MAX_OUTPUT_TOKENS;
        return maxTokens;
    }

    /**
     * ReAct iteration limit. Passed through — quad is the source, and the same number is
     * not interpreted in two places.
     *
     * <p>{@code 0} or negative falls back to the supplied default instead of being passed
     * through: quad rejects such values in {@code Agent.setReActMaxIterations} with an
     * IllegalArgumentException, and the database has a CHECK for it. Passing a 0 on would
     * make the agent fail only at worker startup, far away from the upload.
     */
    static int maxReActIterationsFrom(Integer maxReActIterations, int fallback) {
        if (maxReActIterations == null || maxReActIterations <= 0) {
            return fallback > 0 ? fallback : DEFAULT_MAX_REACT_ITERATIONS;
        }
        return maxReActIterations;
    }

    /**
     * Extract tool names from rich tool definitions.
     * Only the names are preserved — parameters, file permissions and schemas are lost.
     */
    private static Set<String> extractToolNames(List<QuarkusToolDefinition> tools) {
        if (tools == null || tools.isEmpty()) {
            return Set.of();
        }
        return tools.stream()
                .map(QuarkusToolDefinition::name)
                .filter(n -> n != null && !n.isBlank())
                .collect(java.util.stream.Collectors.toCollection(HashSet::new));
    }

    /**
     * Reduced agent config consumed by the Kafka worker.
     *
     * @param agentId internal agent identifier
     * @param agentName human-readable name (optional metadata)
     * @param category optional classification tag
     * @param modelKey LLM model key
     * @param temperature sampling temperature (0–1)
     * @param maxOutputTokens token budget for a single LLM response. <b>Not</b> a ReAct
     *        iteration count. Lands in {@code agent_definitions.max_tokens}.
     * @param allowedTools set of allowed tool names
     * @param systemPrompt system prompt text
     * @param timeoutSeconds task timeout in seconds
     * @param maxRetryAttempts max retry attempts before HITL
     * @param version configuration version
     * @param maxReActIterations how many ReAct rounds the agent may run. Must be
     *        {@code > 0}. Lands in {@code agent_definitions.max_react_iterations} and
     *        reaches the worker as {@code agent.setReActMaxIterations(...)}.
     */
    public record WorkerAgentConfig(
            String agentId,
            String agentName,
            String category,
            String modelKey,
            double temperature,
            int maxOutputTokens,
            Set<String> allowedTools,
            String systemPrompt,
            int timeoutSeconds,
            int maxRetryAttempts,
            int version,
            int maxReActIterations
    ) {
        public WorkerAgentConfig {
            agentId = agentId != null ? agentId : "";
            agentName = agentName != null ? agentName : "";
            category = category != null ? category : "";
            modelKey = modelKey != null ? modelKey : "gpt-4o";
            allowedTools = allowedTools != null ? Set.copyOf(allowedTools) : Set.of();
            systemPrompt = systemPrompt != null ? systemPrompt : "";
            maxReActIterations = maxReActIterations > 0 ? maxReActIterations : DEFAULT_MAX_REACT_ITERATIONS;
        }

        /** Check if this config is complete enough for execution. */
        public boolean isValid() {
            return !agentId.isBlank() && !modelKey.isBlank() && !systemPrompt.isBlank();
        }
    }

    // ── Supporting types (for completeness) ────────────────────────────

    /** Inner representation of a Quarkus agent definition. */
    public record QuarkusAgent(
            String id,
            String name,
            String category,
            String modelId,
            Double temperature,
            Integer maxTokens,
            String systemPrompt,
            Integer timeoutSeconds,
            Integer maxRetryAttempts,
            Integer version,
            Integer maxReActIterations,
            List<QuarkusToolDefinition> toolDefinitions
    ) {
        public QuarkusAgent {
            id = id != null ? id : "";
            name = name != null ? name : "";
            modelId = modelId != null ? modelId : "gpt-4o";
            systemPrompt = systemPrompt != null ? systemPrompt : "";
            // version is nullable but is unpacked in WorkerAgentConfig(int version).
            // Without a default the smallest possible agent definition
            // {"id":"x"} throws a NullPointerException instead of yielding a usable value.
            version = version != null ? version : 1;
        }
    }

    /** Inner representation of a Quarkus tool definition. */
    public record QuarkusToolDefinition(
            String name,
            String description,
            Map<String, Object> parameters,
            Boolean readOnly,
            List<String> allowedDirectories,
            Map<String, Object> permissions
    ) {
        public QuarkusToolDefinition {
            name = name != null ? name : "";
            permissions = permissions != null ? permissions : Map.of();
        }
    }
}
