package de.augmentia.agentflow.app.workflow.declarative;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.agentflow.app.activities.DeclarativeActivitiesImpl;
import de.augmentia.quad.core.observability.AgentMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Authoring path: validate drafts and store runtime overrides. */
class WorkflowDefinitionRegistryAdminTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private WorkflowDefinitionRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new WorkflowDefinitionRegistry(mapper, "classpath*:workflows/*.json");
    }

    @Test
    void rejectsSemanticallyInvalidDraft() throws Exception {
        JsonNode draft = mapper.readTree("""
                { "format":"quad.workflow", "schemaVersion":2,
                  "mainWorkflow": { "id":"broken", "name":"Broken", "version":"1.0.0", "status":"draft",
                    "nodes": [ { "id":"a", "type":"agent", "agent": { "ref":"missing" } } ] },
                  "agents": [] }
                """);
        assertThatThrownBy(() -> registry.validate(draft)).isInstanceOf(RuntimeException.class);
    }

    @Test
    void storesOverrideAndPlansIt() throws Exception {
        JsonNode draft = mapper.readTree("""
                { "format":"quad.workflow", "schemaVersion":2,
                  "mainWorkflow": { "id":"custom", "name":"Custom", "version":"1.0.0", "status":"draft",
                    "nodes": [
                      { "id":"one", "type":"agent", "agent": { "ref":"ex-one" } },
                      { "id":"two", "type":"agent", "dependsOn":["one"], "agent": { "ref":"ex-two" } },
                      { "id":"end", "type":"end", "dependsOn":["two"] } ] },
                  "agents": [
                    { "id":"ex-one", "name":"One", "systemPrompt":"one" },
                    { "id":"ex-two", "name":"Two", "systemPrompt":"two" } ] }
                """);
        JsonNode published = registry.validate(draft);
        WorkflowDefinitionSummary summary = registry.save("custom", published);

        assertThat(summary.id()).isEqualTo("custom");
        assertThat(registry.get("custom")).isPresent();

        DeclarativePlan plan = new DeclarativeActivitiesImpl(registry, new AgentMetrics(new SimpleMeterRegistry())).plan("custom");
        assertThat(plan.nodes()).extracting(NodeSpec::id).contains("one", "two");
    }
}
