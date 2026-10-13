package de.augmentia.quad.worker.agent;

import de.augmentia.agentplatform.common.event.TaskEvent;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TaskPromptBuilderTest {

    private final TaskPromptBuilder builder = new TaskPromptBuilder();

    @Test
    void buildBasicTask() {
        TaskEvent task = createTask("test-workflow", "task-1", "step-1", 0, "Do something");
        
        String prompt = builder.build(task, null);
        
        assertThat(prompt).contains("Workflow: test-workflow");
        assertThat(prompt).contains("Step: step-1 (iteration 0, attempt 0)");
        assertThat(prompt).contains("Task:\nDo something");
        // without def -> plain text output (no output contract section)
        assertThat(prompt).contains("Return your answer as plain text.");
    }

    @Test
    void buildTaskWithContextReferences() {
        TaskEvent.ContextReference ref = new TaskEvent.ContextReference(
            "upstream_result", 
            "step-0", 
            Map.of("key", "value")
        );
        
        TaskEvent task = new TaskEvent(
            new TaskEvent.Header("wf-123", "task-456", null, 0, System.currentTimeMillis()),
            "researcher",
            "step-1",
            0,
            "Analyze code",
            null,
            List.of(ref),
            120000L
        );
        
        String prompt = builder.build(task, null);
        
        assertThat(prompt).contains("Upstream context:");
        assertThat(prompt).contains("upstream_result: {key=value}");
    }

    @Test
    void buildTaskWithInputJson() {
        TaskEvent task = new TaskEvent(
            new TaskEvent.Header("wf-123", "task-456", null, 0, System.currentTimeMillis()),
            "coder",
            "step-2",
            1,
            "Refactor module",
            Map.of("module", "auth", "type", "security"),
            null,
            120000L
        );
        
        String prompt = builder.build(task, null);
        
        assertThat(prompt).contains("Workflow input:");
        assertThat(prompt).contains("module");
    }

    @Test
    void buildTaskWithStructuredOutput() {
        var def = new WorkerAgentDefinition(
            "researcher", "res-1", "You are a researcher.", 
            "simple", 0.2, null, Set.of("readFile"), 8, 120000L, null, true
        );
        
        TaskEvent task = createTask("wf-1", "task-1", "step-1", 0, "Research topic");
        
        String prompt = builder.build(task, def);
        
        assertThat(prompt).contains("Output contract:");
        assertThat(prompt).contains("Return one JSON object with fields: response, confidence, status");
    }

    private TaskEvent createTask(String workflowId, String taskId, String stepId, int iteration, String input) {
        return new TaskEvent(
            new TaskEvent.Header(workflowId, taskId, null, 0, System.currentTimeMillis()),
            "researcher",
            stepId,
            iteration,
            input,
            null,
            null,
            120000L
        );
    }
}