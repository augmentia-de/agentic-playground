package de.augmentia.quad.worker.result;

import com.fasterxml.jackson.core.type.TypeReference;
import de.augmentia.quad.core.agent.AgentResult;
import de.augmentia.agentplatform.common.event.ResultEvent;
import de.augmentia.agentplatform.common.event.TaskEvent;
import de.augmentia.quad.worker.contract.WorkerJson;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps the quad-core AgentResult onto the workflow-conformant ResultEvent.
 */
public class ResultMapper {

    /**
     * Maps a successful result.
     */
    public ResultEvent mapSuccess(TaskEvent task, AgentResult agentResult, String sessionId, long durationMs) {
        Map<String, Object> resultData = new LinkedHashMap<>();
        resultData.put("executor", "quad-core");
        resultData.put("agentType", task.agentType());
        resultData.put("stepId", task.stepId());
        resultData.put("iteration", task.iteration());
        resultData.put("attempt", task.header().attempt());
        resultData.put("sessionId", sessionId);
        resultData.put("durationMs", durationMs);

        // Structured Output parsen falls vorhanden
        if (agentResult.hasStructuredOutput()) {
            try {
                Map<String, Object> structured = WorkerJson.mapper()
                    .readValue(agentResult.structuredOutput(), new TypeReference<Map<String, Object>>() {});
                resultData.putAll(structured);
            } catch (Exception e) {
                // Fallback auf finalAnswer
                resultData.put("response", agentResult.finalAnswer());
            }
        } else if (agentResult.finalAnswer() != null) {
            resultData.put("response", agentResult.finalAnswer());
        }

        // default status if not set by the agent
        if (!resultData.containsKey("status")) {
            resultData.put("status", "COMPLETED");
        }

        return new ResultEvent(
            new ResultEvent.Header(task.header().traceParent(), System.currentTimeMillis()),
            task.header().workflowId(),
            task.header().taskId(),
            task.stepId(),
            task.iteration(),
            task.header().attempt(),
            Instant.ofEpochMilli(System.currentTimeMillis() - durationMs).toEpochMilli(),
            System.currentTimeMillis(),
            resultData
        );
    }

    /**
     * Maps an error result (FAILED_RETRYABLE or FAILED_FATAL).
     */
    public ResultEvent mapFailure(TaskEvent task, Exception ex, long durationMs) {
        Map<String, Object> resultData = new LinkedHashMap<>();
        resultData.put("executor", "quad-core");
        resultData.put("agentType", task.agentType());
        resultData.put("stepId", task.stepId());
        resultData.put("iteration", task.iteration());
        resultData.put("attempt", task.header().attempt());
        resultData.put("durationMs", durationMs);
        
        String errorClass = classifyError(ex);
        resultData.put("status", errorClass.equals("fatal") ? "FAILED_FATAL" : "FAILED_RETRYABLE");
        resultData.put("errorMessage", truncate(ex.getMessage(), 1000));
        resultData.put("errorType", ex.getClass().getSimpleName());

        return new ResultEvent(
            new ResultEvent.Header(task.header().traceParent(), System.currentTimeMillis()),
            task.header().workflowId(),
            task.header().taskId(),
            task.stepId(),
            task.iteration(),
            task.header().attempt(),
            Instant.ofEpochMilli(System.currentTimeMillis() - durationMs).toEpochMilli(),
            System.currentTimeMillis(),
            resultData
        );
    }

    private String classifyError(Exception ex) {
        String msg = ex.getMessage() != null ? ex.getMessage().toLowerCase() : "";
        String type = ex.getClass().getName().toLowerCase();

        // Timeout / Rate-Limit / Netzwerk -> retryable
        if (msg.contains("timeout") || msg.contains("timed out") || 
            msg.contains("429") || msg.contains("rate limit") ||
            type.contains("ioexception") || type.contains("socket")) {
            return "retryable";
        }

        // HTTP Errors  
        if (msg.contains("401") || msg.contains("403")) {
            return "fatal";
        }
        if (msg.contains("5")) {
            return "retryable";
        }

        // default: retryable
        return "retryable";
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return null;
        return s.length() <= maxLen ? s : s.substring(0, maxLen);
    }
}