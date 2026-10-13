package de.augmentia.quad.worker.contract;

import de.augmentia.agentplatform.common.event.ResultEvent;
import de.augmentia.agentplatform.common.event.TaskEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ContractSerializationTest {

    @Test
    void serializeDeserializeTaskEvent() {
        TaskEvent task = new TaskEvent(
            new TaskEvent.Header("workflow-123", "task-456", "trace-parent-xyz", 2, Instant.now().toEpochMilli()),
            "researcher",
            "step-code-review",
            0,
            "Review the code changes and identify security issues",
            Map.of("repository", "my-app", "branch", "feature/auth"),
            List.of(new TaskEvent.ContextReference("baseline", "step-initial", Map.of("issues", 0))),
            180000L
        );
        
        String json = WorkerJson.write(task);
        
        assertThat(json).contains("\"agentType\":\"researcher\"");
        assertThat(json).contains("\"taskId\":\"task-456\"");
        
        TaskEvent deserialized = WorkerJson.read(json, TaskEvent.class);
        
        assertThat(deserialized.header().workflowId()).isEqualTo("workflow-123");
        assertThat(deserialized.header().taskId()).isEqualTo("task-456");
        assertThat(deserialized.agentType()).isEqualTo("researcher");
        assertThat(deserialized.stepId()).isEqualTo("step-code-review");
        assertThat(deserialized.inputPrompt()).isEqualTo("Review the code changes and identify security issues");
        assertThat(deserialized.contextReferences()).hasSize(1);
        assertThat(deserialized.timeoutMs()).isEqualTo(180000L);
    }

    @Test
    void serializeDeserializeResultEvent() {
        ResultEvent result = new ResultEvent(
            new ResultEvent.Header("trace-abc", Instant.now().toEpochMilli()),
            "workflow-123",
            "task-456",
            "step-research",
            0,
            1,
            Instant.now().minusSeconds(5).toEpochMilli(),
            Instant.now().toEpochMilli(),
            Map.of(
                "executor", "quad-core",
                "agentType", "researcher",
                "response", "Found 3 security issues",
                "confidence", 0.92,
                "status", "COMPLETED"
            )
        );
        
        String json = WorkerJson.write(result);
        
        assertThat(json).contains("\"workflowId\":\"workflow-123\"");
        assertThat(json).contains("\"agentType\":\"researcher\"");
        assertThat(json).contains("\"confidence\":0.92");
        
        ResultEvent deserialized = WorkerJson.read(json, ResultEvent.class);
        
        assertThat(deserialized.workflowId()).isEqualTo("workflow-123");
        assertThat(deserialized.resultData().get("confidence")).isEqualTo(0.92);
        assertThat(deserialized.resultData().get("status")).isEqualTo("COMPLETED");
    }

    @Test
    void deserializeMissingOptionalFields() {
        String minimalJson = """
            {
              "header": {"workflowId": "wf-1", "taskId": "t-1"},
              "agentType": "coder",
              "stepId": "step-1",
              "iteration": 0
            }
            """;
        
        TaskEvent task = WorkerJson.read(minimalJson, TaskEvent.class);
        
        // fields without a default are null — that is ok, the executor validates them later
        assertThat(task.header().workflowId()).isEqualTo("wf-1");
        assertThat(task.agentType()).isEqualTo("coder");
        assertThat(task.inputPrompt()).isNull();
    }
}