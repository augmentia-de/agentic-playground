package de.augmentia.agentflow.app.workflow.declarative;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class WorkflowDefinitionStoreTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void persistsOverridesAndKeepsVersions(@TempDir Path dir) throws Exception {
        WorkflowDefinitionStore store = new WorkflowDefinitionStore(mapper, dir.toString());
        JsonNode doc = mapper.readTree("{\"mainWorkflow\":{\"id\":\"x\",\"name\":\"X\"}}");

        store.persist(doc);
        assertThat(store.loadOverrides()).hasSize(1);
        assertThat(store.versions("x")).isEmpty();

        store.persist(doc);
        assertThat(store.versions("x")).hasSize(1);
        assertThat(store.version("x", store.versions("x").get(0))).isPresent();
    }

    @Test
    void disabledWhenStateDirBlank() {
        WorkflowDefinitionStore store = new WorkflowDefinitionStore(mapper, "");
        assertThat(store.enabled()).isFalse();
        store.persist(mapper.createObjectNode());
        assertThat(store.loadOverrides()).isEmpty();
        assertThat(store.versions("x")).isEmpty();
    }

    @Test
    void rejectsTraversalInVersionName(@TempDir Path dir) {
        WorkflowDefinitionStore store = new WorkflowDefinitionStore(mapper, dir.toString());
        assertThat(store.version("x", "../secret")).isEmpty();
    }
}
