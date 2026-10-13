package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.model.DagNode;
import de.augmentia.agentplatform.common.model.WorkflowDefinition;
import de.augmentia.agentplatform.common.routing.WorkflowDefinitionValidator;
import de.augmentia.agentplatform.orchestrator.definition.WorkflowDefinitionUpload;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class WorkflowDefinitionResourceTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "workflows/reference-code-quality.json",
            "workflows/xor-race.json",
            "workflows/n-of-quorum.json",
            "workflows/guard-skip.json",
            "workflows/error-taxonomy.json"
    })
    void seededDefinitionsAreValid(String resource) throws Exception {
        WorkflowDefinitionUpload upload = read(resource);
        WorkflowDefinition definition = upload.toDefinition(UUID.randomUUID().toString(),
                upload.version() == null ? 1 : upload.version(), true);

        assertThat(WorkflowDefinitionValidator.validate(definition)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"workflows/reference-code-quality.json"})
    void mapsSpecLoopBodyAgentType(String resource) throws Exception {
        WorkflowDefinitionUpload upload = read(resource);
        WorkflowDefinition definition = upload.toDefinition(UUID.randomUUID().toString(),
                upload.version() == null ? 1 : upload.version(), true);

        DagNode loop = definition.node("refinement_loop");
        assertThat(loop.agentType()).isEqualTo("refiner");
        assertThat(loop.maxIterations()).isEqualTo(3);
    }

    private WorkflowDefinitionUpload read(String resource) throws Exception {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(resource)) {
            assertThat(input).as("Resource %s should exist", resource).isNotNull();
            String json = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            return EventJson.read(json, WorkflowDefinitionUpload.class);
        }
    }
}
