package de.augmentia.quad.worker.agent;

import de.augmentia.agentplatform.common.event.TaskEvent;

/**
 * Assembles prompts from TaskEvents and AgentDefinitions.
 */
public class TaskPromptBuilder {

    /**
     * Builds the final prompt for a task.
     * Contains workflow context, step info, input and upstream context.
     */
    public String build(TaskEvent task, WorkerAgentDefinition def) {
        StringBuilder sb = new StringBuilder();
        
        // Header info
        sb.append("Workflow: %s\n".formatted(task.header().workflowId()));
        sb.append("Execution: %s\n".formatted(task.header().workflowId()));
        sb.append("Step: %s (iteration %d, attempt %d)\n"
            .formatted(task.stepId(), task.iteration(), task.header().attempt()));
        sb.append("Agent: %s\n\n".formatted(task.agentType()));
        
        // User input
        sb.append("Task:\n");
        sb.append(task.inputPrompt() != null ? task.inputPrompt() : "No specific instructions.");
        sb.append("\n\n");
        
        // Workflow input JSON falls vorhanden
        if (task.inputJson() != null && !task.inputJson().isEmpty()) {
            sb.append("Workflow input:\n");
            sb.append(task.inputJson());
            sb.append("\n\n");
        }
        
        // Upstream context references
        if (task.contextReferences() != null && !task.contextReferences().isEmpty()) {
            sb.append("Upstream context:\n");
            for (var ref : task.contextReferences()) {
                sb.append("- ").append(ref.name()).append(": ");
                if (ref.resultData() != null && !ref.resultData().isEmpty()) {
                    sb.append(ref.resultData());
                } else {
                    sb.append("(empty)");
                }
                sb.append("\n");
            }
            sb.append("\n");
        }
        
        // Output contract
        boolean structuredOutput = def != null && def.structuredOutputEnabled();
        if (structuredOutput) {
            sb.append("Output contract:\n");
            sb.append("Return one JSON object with fields: response, confidence, status.\n");
        } else {
            sb.append("Return your answer as plain text.\n");
        }
        
        return sb.toString();
    }
}