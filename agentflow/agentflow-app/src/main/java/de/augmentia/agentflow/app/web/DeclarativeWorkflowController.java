package de.augmentia.agentflow.app.web;

import com.fasterxml.jackson.databind.JsonNode;
import de.augmentia.agentflow.app.workflow.declarative.DeclarativeAgentWorkflow;
import de.augmentia.agentflow.app.workflow.declarative.WorkflowDefinitionRegistry;
import de.augmentia.agentflow.app.workflow.declarative.WorkflowDefinitionSummary;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Exposes declarative workflow definitions (v2 QUAD documents) and runs them durably.
 * Definitions are loaded/validated at startup by {@link WorkflowDefinitionRegistry}.
 */
@RestController
public class DeclarativeWorkflowController {

    static final String TASK_QUEUE = "agentflow";

    private final WorkflowClient workflowClient;
    private final WorkflowDefinitionRegistry registry;

    public DeclarativeWorkflowController(WorkflowClient workflowClient, WorkflowDefinitionRegistry registry) {
        this.workflowClient = workflowClient;
        this.registry = registry;
    }

    /** Lists the loaded workflow definitions. */
    @GetMapping("/agentflow/workflows")
    public List<WorkflowDefinitionSummary> list() {
        return registry.list();
    }

    /** Returns the full published v2 document (with content hashes) for one definition. */
    @GetMapping("/agentflow/workflows/{id}")
    public JsonNode get(@PathVariable("id") String id) {
        return registry.get(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown workflow: " + id));
    }

    /** Runs a declarative workflow definition durably. */
    @PostMapping("/agentflow/runs/declarative")
    public Map<String, Object> run(@RequestParam("workflowId") String workflowId,
                                   @RequestParam("task") String task) {
        registry.require(workflowId); // fail fast on unknown ids
        String temporalId = "agentflow-decl-" + UUID.randomUUID();
        DeclarativeAgentWorkflow workflow = workflowClient.newWorkflowStub(
                DeclarativeAgentWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(temporalId).build());
        WorkflowClient.start(workflow::run, workflowId, task);
        return Map.of("workflowId", temporalId, "definitionId", workflowId, "started", true,
                "hint", "nodes with requiresHumanApproval wait for .../{workflowId}/approve");
    }

    /** Human approval signal for declarative runs with {@code requiresHumanApproval} nodes. */
    @PostMapping("/agentflow/runs/declarative/{workflowId}/approve")
    public Map<String, Object> approve(@PathVariable("workflowId") String workflowId,
                                       @RequestParam("approved") boolean approved) {
        workflowClient.newWorkflowStub(DeclarativeAgentWorkflow.class, workflowId).approve(approved);
        return Map.of("workflowId", workflowId, "signalled", "approve", "approved", approved);
    }
}
