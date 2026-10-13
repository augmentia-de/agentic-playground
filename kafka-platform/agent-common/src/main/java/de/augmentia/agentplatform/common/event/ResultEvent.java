package de.augmentia.agentplatform.common.event;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Canonical result contract that the QUAD worker publishes on {@code agent.execution.results}.
 *
 * <p>{@code resultData} carries the status under {@code "status"} ({@code COMPLETED},
 * {@code FAILED_RETRYABLE}, {@code FAILED_FATAL}, {@code NEEDS_HITL}) as well as optional
 * {@code errorMessage}/{@code errorType} fields.
 */
public record ResultEvent(
        Header header,
        String workflowId,
        String taskId,
        String stepId,
        int iteration,
        int attempt,
        long startedAtMs,
        long finishedAtMs,
        @JsonProperty("result_data") Map<String, Object> resultData
) {

    public record Header(
            String traceParent,
            long timestamp
    ) {}
}