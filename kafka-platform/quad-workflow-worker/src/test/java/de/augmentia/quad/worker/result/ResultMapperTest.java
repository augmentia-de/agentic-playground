package de.augmentia.quad.worker.result;

import de.augmentia.agentplatform.common.event.ResultEvent;
import de.augmentia.agentplatform.common.event.TaskEvent;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ResultMapperTest {

    private final ResultMapper mapper = new ResultMapper();

    @Test
    void mapFailureRetryableForTimeout() {
        TaskEvent task = createTask("wf-1", "task-1", "step-1", 0, 1);
        Exception ex = new RuntimeException("Timeout connecting to LLM provider");
        
        ResultEvent result = mapper.mapFailure(task, ex, 5000L);
        
        assertThat(result.resultData().get("status")).isEqualTo("FAILED_RETRYABLE");
        assertThat(result.resultData().get("errorType")).isEqualTo("RuntimeException");
        assertThat((String) result.resultData().get("errorMessage")).contains("Timeout");
    }

    @Test
    void mapFailureFatalForAuthError() {
        TaskEvent task = createTask("wf-1", "task-1", "step-1", 0, 2);
        Exception ex = new RuntimeException("HTTP 401 Unauthorized: Invalid API key");
        
        ResultEvent result = mapper.mapFailure(task, ex, 1000L);
        
        assertThat(result.resultData().get("status")).isEqualTo("FAILED_FATAL");
    }

    @Test
    void mapFailureFatalForForbidden() {
        TaskEvent task = createTask("wf-1", "task-1", "step-1", 0, 1);
        Exception ex = new RuntimeException("HTTP 403 Forbidden: Insufficient permissions");
        
        ResultEvent result = mapper.mapFailure(task, ex, 2000L);
        
        assertThat(result.resultData().get("status")).isEqualTo("FAILED_FATAL");
    }

    @Test
    void mapFailureRetryableForRateLimit() {
        TaskEvent task = createTask("wf-1", "task-1", "step-1", 0, 1);
        Exception ex = new RuntimeException("HTTP 429 Too Many Requests - rate limit exceeded");
        
        ResultEvent result = mapper.mapFailure(task, ex, 300L);
        
        assertThat(result.resultData().get("status")).isEqualTo("FAILED_RETRYABLE");
    }

    @Test
    void mapFailureDefaultIsRetryable() {
        TaskEvent task = createTask("wf-1", "task-1", "step-1", 0, 1);
        Exception ex = new RuntimeException("Something unexpected happened");
        
        ResultEvent result = mapper.mapFailure(task, ex, 100L);
        
        // the default is retryable (safe assumption for unknown errors)
        assertThat(result.resultData().get("status")).isEqualTo("FAILED_RETRYABLE");
    }

    @Test
    void resultEventContainsAllRequiredFields() {
        TaskEvent task = createTask("wf-xyz", "task-abc", "step-research", 2, 3);
        Exception ex = new RuntimeException("Test error");
        
        ResultEvent result = mapper.mapFailure(task, ex, 7000L);
        
        assertThat(result.workflowId()).isEqualTo("wf-xyz");
        assertThat(result.taskId()).isEqualTo("task-abc");
        assertThat(result.stepId()).isEqualTo("step-research");
        assertThat(result.iteration()).isEqualTo(2);
        assertThat(result.attempt()).isEqualTo(3);
        assertThat(result.startedAtMs()).isLessThan(result.finishedAtMs());
        assertThat(result.resultData().get("executor")).isEqualTo("quad-core");
        assertThat(result.resultData().get("agentType")).isEqualTo("researcher");
    }

    private TaskEvent createTask(String workflowId, String taskId, String stepId, int iteration, int attempt) {
        return new TaskEvent(
            new TaskEvent.Header(workflowId, taskId, "trace-parent-123", attempt, System.currentTimeMillis()),
            "researcher",
            stepId,
            iteration,
            "Test input prompt",
            null,
            null,
            120000L
        );
    }
}