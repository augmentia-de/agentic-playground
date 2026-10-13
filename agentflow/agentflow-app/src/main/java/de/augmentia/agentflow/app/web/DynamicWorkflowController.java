package de.augmentia.agentflow.app.web;

import com.fasterxml.jackson.databind.JsonNode;
import de.augmentia.agentflow.app.workflow.declarative.DeclarativeAgentWorkflow;
import de.augmentia.agentflow.app.workflow.declarative.WorkflowDefinitionRegistry;
import de.augmentia.agentflow.app.workflow.declarative.WorkflowDefinitionStore;
import de.augmentia.agentflow.app.workflow.declarative.WorkflowDefinitionSummary;
import de.augmentia.agentflow.app.workflow.generate.WorkflowGeneratorService;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;
import java.util.UUID;

/**
 * Dynamic workflow creation: an LLM decomposes a task into a v2 workflow document that can be
 * previewed, saved (appears in the Workflows/Agents pages, persisted to the state dir) and run
 * durably via the existing Temporal engine.
 *
 * <p>Endpoints (all under {@code /agentflow/workflows/generate}):</p>
 * <ul>
 *   <li>{@code POST /generate} — task → published v2 document (preview, not persisted)</li>
 *   <li>{@code POST /generate/save} — generate + persist as a new workflow (own id)</li>
 *   <li>{@code POST /generate/run} — generate + run it durably on Temporal</li>
 * </ul>
 */
@RestController
public class DynamicWorkflowController {

    static final String TASK_QUEUE = "agentflow";

    private final WorkflowGeneratorService generator;
    private final WorkflowDefinitionRegistry registry;
    private final WorkflowDefinitionStore store;
    private final WorkflowClient workflowClient;

    public DynamicWorkflowController(WorkflowGeneratorService generator,
                                     WorkflowDefinitionRegistry registry,
                                     WorkflowDefinitionStore store,
                                     WorkflowClient workflowClient) {
        this.generator = generator;
        this.registry = registry;
        this.store = store;
        this.workflowClient = workflowClient;
    }

    /** request: { task, name? } */
    public record GenerateRequest(String task, String name) {}

    /** Decomposes a task into a v2 workflow document for preview (not persisted). */
    @PostMapping("/agentflow/workflows/generate")
    public Map<String, Object> generate(@RequestBody GenerateRequest req) {
        requireTask(req);
        JsonNode workflow = generateOrThrow(req);
        return Map.of("workflow", workflow);
    }

    /** Generates a workflow and persists it as a new definition (runtime override + file). */
    @PostMapping("/agentflow/workflows/generate/save")
    public WorkflowDefinitionSummary save(@RequestBody GenerateRequest req) {
        requireTask(req);
        JsonNode published = generateOrThrow(req);
        String id = published.path("mainWorkflow").path("id").asText();
        WorkflowDefinitionSummary summary = registry.save(id, published);
        store.persist(published);
        return summary;
    }

    /** Generates a workflow, registers it at runtime and runs it durably on Temporal. */
    @PostMapping("/agentflow/workflows/generate/run")
    public Map<String, Object> run(@RequestBody GenerateRequest req) {
        requireTask(req);
        JsonNode published = generateOrThrow(req);
        String definitionId = published.path("mainWorkflow").path("id").asText();
        // Runtime registration so the Temporal worker can resolve the freshly generated definition.
        registry.save(definitionId, published);

        String temporalId = "agentflow-decl-" + UUID.randomUUID();
        DeclarativeAgentWorkflow workflow = workflowClient.newWorkflowStub(
                DeclarativeAgentWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(temporalId).build());
        WorkflowClient.start(workflow::run, definitionId, req.task());
        return Map.of("workflowId", temporalId, "definitionId", definitionId, "started", true,
                "hint", "durable run; see Runs/Temporal UI. Nodes with requiresHumanApproval wait for approval.");
    }

    private JsonNode generateOrThrow(GenerateRequest req) {
        try {
            return generator.generate(req.task(), req.name());
        } catch (RuntimeException e) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY,
                    "Workflow generation failed: " + e.getMessage(), e);
        }
    }

    private void requireTask(GenerateRequest req) {
        if (req == null || req.task() == null || req.task().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "task required");
        }
    }
}
