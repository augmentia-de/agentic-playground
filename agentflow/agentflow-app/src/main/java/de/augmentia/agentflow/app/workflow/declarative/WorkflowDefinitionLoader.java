package de.augmentia.agentflow.app.workflow.declarative;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.quad.core.workflow.transfer.QuadWorkflowPublishCompiler;
import de.augmentia.quad.core.workflow.transfer.QuadWorkflowSemanticValidator;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Loads a v2 QUAD workflow document: resolves its inline {@code agents[]}, runs the
 * quad-core publish compiler (fills {@code contentHash}, sets {@code status: published},
 * validates against the v2 JSON schema) and applies the semantic validator (single END,
 * acyclic {@code dependsOn}, resolvable references). Reuses {@code quad-workflow-core}.
 */
public class WorkflowDefinitionLoader {

    private final ObjectMapper mapper;
    private final QuadWorkflowPublishCompiler compiler = new QuadWorkflowPublishCompiler();

    public WorkflowDefinitionLoader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /** Parses, publishes and validates a v2 draft; returns the published document. */
    public JsonNode load(InputStream in) throws Exception {
        return load(mapper.readTree(in));
    }

    /** Publishes and validates an already parsed v2 draft; returns the published document. */
    public JsonNode load(JsonNode draft) {
        JsonNode published = compiler.publish(draft, ref -> resolve(agentsById(draft), ref));

        List<String> semantic = QuadWorkflowSemanticValidator.validate(published);
        if (!semantic.isEmpty()) {
            throw new IllegalStateException("semantically invalid workflow: " + String.join("; ", semantic));
        }
        return published;
    }

    private Optional<JsonNode> resolve(Map<String, JsonNode> agents, String ref) {
        if (ref == null) {
            return Optional.empty();
        }
        JsonNode direct = agents.get(ref);
        if (direct != null) {
            return Optional.of(direct);
        }
        int at = ref.indexOf('@'); // allow 'agents/<id>@1.2.3' style refs
        if (at > 0) {
            return Optional.ofNullable(agents.get(ref.substring(0, at)));
        }
        return Optional.empty();
    }

    private Map<String, JsonNode> agentsById(JsonNode draft) {
        Map<String, JsonNode> map = new HashMap<>();
        if (draft.path("agents").isArray()) {
            for (JsonNode agent : draft.get("agents")) {
                String id = agent.path("id").asText(null);
                if (id != null && !id.isBlank()) {
                    map.put(id, agent);
                }
            }
        }
        return map;
    }
}
