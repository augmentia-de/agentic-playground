package de.augmentia.agentflow.app.workflow.declarative;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.agentflow.app.activities.DeclarativeActivitiesImpl;
import de.augmentia.quad.core.observability.AgentMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Flattening/planning incl. sub-workflow expansion and all node types. */
class DeclarativePlannerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private WorkflowDefinitionRegistry registry;
    private DeclarativeActivitiesImpl activities;

    @BeforeEach
    void setUp() {
        registry = new WorkflowDefinitionRegistry(mapper, "classpath*:workflows/*.json");
        activities = new DeclarativeActivitiesImpl(registry, new AgentMetrics(new SimpleMeterRegistry()));
    }

    @Test
    void listsAllDefinitions() {
        assertThat(registry.list()).extracting(WorkflowDefinitionSummary::id)
                .containsExactlyInAnyOrder("support-triage", "contract-review", "invoice-processing",
                        "market-research", "onboarding");
    }

    @Test
    void plansMarketResearch() {
        DeclarativePlan plan = activities.plan("market-research");

        assertThat(ids(plan)).contains("scout", "analyst", "writer", "end");
        assertThat(byId(plan, "scout").tools()).contains("webSearch", "webfetch");
        assertThat(byId(plan, "analyst").dependsOn()).contains("scout");
        assertThat(byId(plan, "end").type()).isEqualTo("end");
    }

    @Test
    void supportTriageClassifyRequiresApproval() {
        DeclarativePlan plan = activities.plan("support-triage");

        assertThat(byId(plan, "classify").requiresHumanApproval()).isTrue();
        assertThat(byId(plan, "playbook").dependsOn()).contains("classify");
    }

    @Test
    void expandsSubWorkflowAndAliasesEnd() throws Exception {
        JsonNode draft = mapper.readTree("""
                { "format":"quad.workflow", "schemaVersion":2,
                  "mainWorkflow": { "id":"parent", "name":"Parent", "version":"1.0.0", "status":"draft",
                    "nodes": [
                      { "id":"start", "type":"agent", "agent": { "ref":"ex-start" } },
                      { "id":"sub", "type":"subworkflow", "workflowRef":"child", "dependsOn":["start"] },
                      { "id":"after", "type":"agent", "dependsOn":["sub"], "agent": { "ref":"ex-after" } },
                      { "id":"end", "type":"end", "dependsOn":["after"] } ] },
                  "subWorkflows": [
                    { "id":"child", "name":"Child", "version":"1.0.0", "status":"draft",
                      "nodes": [
                        { "id":"one", "type":"agent", "agent": { "ref":"ex-one" } },
                        { "id":"end", "type":"end", "dependsOn":["one"] } ] } ],
                  "agents": [
                    { "id":"ex-start", "name":"Start", "systemPrompt":"start" },
                    { "id":"ex-after", "name":"After", "systemPrompt":"after" },
                    { "id":"ex-one", "name":"One", "systemPrompt":"one" } ] }
                """);
        registry.save("parent", registry.validate(draft));

        DeclarativePlan plan = activities.plan("parent");

        assertThat(ids(plan)).contains("start", "sub/one", "sub/end", "after", "end");
        // inner root depends on the sub-workflow node's external dependency
        assertThat(byId(plan, "sub/one").dependsOn()).contains("start");
        // external reference to the sub-workflow node is rewritten to the inlined end
        assertThat(byId(plan, "after").dependsOn()).contains("sub/end");
    }

    @Test
    void propagatesGuardrailsAndHooksIntoNodeSpec() throws Exception {
        JsonNode draft = mapper.readTree("""
                { "format":"quad.workflow", "schemaVersion":2,
                  "mainWorkflow": { "id":"guarded", "name":"Guarded", "version":"1.0.0", "status":"draft",
                    "nodes": [
                      { "id":"a", "type":"agent", "agent": { "ref":"ex-guarded" } },
                      { "id":"end", "type":"end", "dependsOn":["a"] } ] },
                  "agents": [
                    { "id":"ex-guarded", "name":"Guarded", "systemPrompt":"p",
                      "guardrailsInput":["prompt-injection","sql-injection"],
                      "guardrailsOutput":["pii"],
                      "hooks":["pii:EMAIL,PHONE_NUMBER","audit"] } ] }
                """);
        registry.save("guarded", registry.validate(draft));

        NodeSpec a = byId(activities.plan("guarded"), "a");
        assertThat(a.guardrailsInput()).containsExactly("prompt-injection", "sql-injection");
        assertThat(a.guardrailsOutput()).containsExactly("pii");
        assertThat(a.hooks()).containsExactly("pii:EMAIL,PHONE_NUMBER", "audit");
    }

    private java.util.List<String> ids(DeclarativePlan plan) {
        return plan.nodes().stream().map(NodeSpec::id).toList();
    }

    private NodeSpec byId(DeclarativePlan plan, String id) {
        return plan.nodes().stream().filter(n -> n.id().equals(id)).findFirst().orElseThrow();
    }
}
