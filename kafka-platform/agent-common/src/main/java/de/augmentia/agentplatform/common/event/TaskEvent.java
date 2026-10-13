package de.augmentia.agentplatform.common.event;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Canonical task contract that is published by the orchestrator on {@code agent.tasks.<agent_type>}.
 *
 * <p>Uses {@code com.fasterxml.jackson.annotation.JsonProperty} (by both the Jackson 2 and
 * Jackson 3 mappers) and is thereby wire-compatible for the orchestrator (`EventJson`) and
 * the QUAD worker (`WorkerJson`).
 */
public record TaskEvent(
        Header header,
        String agentType,
        String stepId,
        int iteration,
        String inputPrompt,
        Map<String, Object> inputJson,
        List<ContextReference> contextReferences,
        @JsonProperty("timeout_ms") Long timeoutMs
) {

    public record Header(
            String workflowId,
            String taskId,
            String traceParent,
            int attempt,
            long timestamp
    ) {}

    public record ContextReference(
            String name,
            String sourceStepId,
            Map<String, Object> resultData
    ) {}
}