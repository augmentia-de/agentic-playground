package de.augmentia.agentflow.app.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.agentflow.app.workflow.declarative.WorkflowDefinitionRegistry;
import de.augmentia.agentflow.app.workflow.declarative.WorkflowDefinitionStore;
import de.augmentia.agentflow.app.workflow.declarative.WorkflowDefinitionSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Creating empty declarative workflows for authoring. */
class WorkflowAdminControllerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private WorkflowDefinitionRegistry registry;
    private WorkflowAdminController controller;

    @BeforeEach
    void setUp() {
        registry = new WorkflowDefinitionRegistry(mapper, "classpath*:workflows/*.json");
        WorkflowDefinitionStore store = new WorkflowDefinitionStore(mapper, ""); // disabled persistence
        controller = new WorkflowAdminController(registry, store, mapper);
    }

    @Test
    void createsEmptyWorkflowWithSingleEndNode() {
        WorkflowDefinitionSummary summary =
                controller.create(new WorkflowAdminController.CreateWorkflowRequest(null, "My Flow"));

        assertThat(summary.id()).isEqualTo("wf-my-flow");
        assertThat(summary.name()).isEqualTo("My Flow");
        assertThat(summary.nodeCount()).isEqualTo(1);
        assertThat(registry.get("wf-my-flow")).isPresent();
        assertThat(registry.require("wf-my-flow").path("mainWorkflow").path("nodes").get(0).path("type").asText())
                .isEqualTo("end");
    }

    @Test
    void derivesUniqueIds() {
        controller.create(new WorkflowAdminController.CreateWorkflowRequest(null, "Dup"));
        WorkflowDefinitionSummary second =
                controller.create(new WorkflowAdminController.CreateWorkflowRequest(null, "Dup"));

        assertThat(second.id()).isEqualTo("wf-dup-2");
    }

    @Test
    void honoursExplicitId() {
        WorkflowDefinitionSummary summary =
                controller.create(new WorkflowAdminController.CreateWorkflowRequest("explicit-id", "Named"));

        assertThat(summary.id()).isEqualTo("explicit-id");
    }
}
