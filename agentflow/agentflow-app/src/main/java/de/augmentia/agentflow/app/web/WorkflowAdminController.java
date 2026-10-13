package de.augmentia.agentflow.app.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.augmentia.agentflow.app.workflow.declarative.WorkflowDefinitionRegistry;
import de.augmentia.agentflow.app.workflow.declarative.WorkflowDefinitionStore;
import de.augmentia.agentflow.app.workflow.declarative.WorkflowDefinitionSummary;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Authoring API for declarative workflow definitions: validate a v2 draft and store it
 * (runtime override + file persistence with version history). Both use the same publish
 * compiler + semantic validator as startup loading.
 */
@RestController
public class WorkflowAdminController {

    private final WorkflowDefinitionRegistry registry;
    private final WorkflowDefinitionStore store;
    private final ObjectMapper mapper;

    public WorkflowAdminController(WorkflowDefinitionRegistry registry, WorkflowDefinitionStore store,
                                   ObjectMapper mapper) {
        this.registry = registry;
        this.store = store;
        this.mapper = mapper;
    }

    /** request: { id?, name? } — both optional (id derived from the name). */
    public record CreateWorkflowRequest(String id, String name) {}

    /**
     * Creates a new, empty declarative workflow (single {@code end} node) and persists it, so it
     * can be edited in the UI and then extended with agents/nodes.
     */
    @PostMapping("/agentflow/workflows")
    public WorkflowDefinitionSummary create(@RequestBody(required = false) CreateWorkflowRequest req) {
        String name = (req != null && req.name() != null && !req.name().isBlank())
                ? req.name().trim() : "Neuer Workflow";
        String requested = (req != null && req.id() != null && !req.id().isBlank())
                ? req.id().trim() : "wf-" + slug(name);
        String id = uniqueId(requested);

        ObjectNode doc = mapper.createObjectNode();
        doc.put("format", "quad.workflow");
        doc.put("schemaVersion", 2);
        ObjectNode main = doc.putObject("mainWorkflow");
        main.put("id", id);
        main.put("name", name);
        main.put("version", "1.0.0");
        main.put("status", "draft");
        ObjectNode end = main.putArray("nodes").addObject();
        end.put("id", "end");
        end.put("type", "end");
        doc.putArray("agents");

        try {
            JsonNode published = registry.validate(doc);
            WorkflowDefinitionSummary summary = registry.save(id, published);
            store.persist(published);
            return summary;
        } catch (RuntimeException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        }
    }

    private String uniqueId(String base) {
        String id = base;
        int n = 2;
        while (registry.get(id).isPresent()) {
            id = base + "-" + n++;
        }
        return id;
    }

    private static String slug(String name) {
        String s = name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-").replaceAll("(^-+)|(-+$)", "");
        return s.isBlank() ? "workflow" : s;
    }

    /** Validates a v2 draft without storing it. */
    @PostMapping("/agentflow/workflows/validate")
    public Map<String, Object> validate(@RequestBody JsonNode draft) {
        try {
            JsonNode published = registry.validate(draft);
            return Map.of("valid", true, "summary", summarize(published));
        } catch (RuntimeException e) {
            return Map.of("valid", false, "errors", List.of(String.valueOf(e.getMessage())));
        }
    }

    /**
     * Publishes a v2 draft: stores it as a runtime override, persists it to the state dir and
     * snapshots the previous version.
     */
    @PutMapping("/agentflow/workflows/{id}")
    public WorkflowDefinitionSummary save(@PathVariable("id") String id, @RequestBody JsonNode draft) {
        try {
            JsonNode published = registry.validate(draft);
            WorkflowDefinitionSummary summary = registry.save(id, published);
            store.persist(published);
            return summary;
        } catch (RuntimeException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        }
    }

    /** Lists the stored version snapshots for a definition. */
    @GetMapping("/agentflow/workflows/{id}/versions")
    public List<String> versions(@PathVariable("id") String id) {
        return store.versions(id);
    }

    /** Returns a specific version snapshot. */
    @GetMapping("/agentflow/workflows/{id}/versions/{version}")
    public JsonNode version(@PathVariable("id") String id, @PathVariable("version") String version) {
        return store.version(id, version)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown version: " + version));
    }

    private Map<String, Object> summarize(JsonNode published) {
        JsonNode wf = published.path("mainWorkflow");
        return Map.of(
                "id", wf.path("id").asText(),
                "name", wf.path("name").asText(),
                "nodeCount", wf.path("nodes").size());
    }
}
