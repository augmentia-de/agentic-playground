package de.augmentia.agentflow.app.workflow.declarative;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Loads and validates all declarative workflow definitions at startup (fail-closed) and
 * exposes them by {@code mainWorkflow.id}.
 *
 * <p>Location defaults to {@code classpath*:workflows/*.json} and can be overridden via
 * {@code agentflow.workflows.location} (e.g. {@code file:./workflows/*.json}).</p>
 */
@Component
public class WorkflowDefinitionRegistry {

    private static final Logger log = LoggerFactory.getLogger(WorkflowDefinitionRegistry.class);

    private final Map<String, JsonNode> published = new LinkedHashMap<>();
    private final WorkflowDefinitionLoader loader;

    public WorkflowDefinitionRegistry(ObjectMapper mapper,
                                      @Value("${agentflow.workflows.location:classpath*:workflows/*.json}") String location) {
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        this.loader = new WorkflowDefinitionLoader(mapper);
        try {
            Resource[] resources = resolver.getResources(location);
            Arrays.sort(resources, Comparator.comparing(r -> {
                String name = r.getFilename();
                return name == null ? "" : name;
            }));
            for (Resource resource : resources) {
                try (InputStream in = resource.getInputStream()) {
                    JsonNode doc = loader.load(in);
                    String id = doc.path("mainWorkflow").path("id").asText();
                    published.put(id, doc);
                    log.info("Loaded workflow definition '{}' from {}", id, resource.getFilename());
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load workflow definitions from " + location, e);
        }
        if (published.isEmpty()) {
            log.warn("No workflow definitions found at {}", location);
        }
    }

    public List<WorkflowDefinitionSummary> list() {
        List<WorkflowDefinitionSummary> out = new ArrayList<>();
        published.values().forEach(doc -> out.add(summarize(doc)));
        return out;
    }

    public Optional<JsonNode> get(String id) {
        return Optional.ofNullable(published.get(id));
    }

    public JsonNode require(String id) {
        JsonNode doc = published.get(id);
        if (doc == null) {
            throw new IllegalArgumentException("Unknown workflow definition: " + id + " (known: " + published.keySet() + ")");
        }
        return doc;
    }

    /** Publishes + validates a draft without storing it (for the editor's "Validate" action). */
    public JsonNode validate(JsonNode draft) {
        return loader.load(draft);
    }

    /** Stores a validated/published document as a runtime override (until restart). */
    public WorkflowDefinitionSummary save(String id, JsonNode publishedDoc) {
        String docId = publishedDoc.path("mainWorkflow").path("id").asText();
        if (docId == null || docId.isBlank()) {
            throw new IllegalArgumentException("document has no mainWorkflow.id");
        }
        if (id != null && !id.isBlank() && !id.equals(docId)) {
            throw new IllegalArgumentException("path id '" + id + "' does not match document id '" + docId + "'");
        }
        published.put(docId, publishedDoc);
        log.info("Stored runtime override for workflow definition '{}'", docId);
        return summarize(publishedDoc);
    }

    /** Registers an already-published document (used by the startup override bootstrap). */
    public void register(JsonNode publishedDoc) {
        String docId = publishedDoc.path("mainWorkflow").path("id").asText();
        if (docId != null && !docId.isBlank()) {
            published.put(docId, publishedDoc);
        }
    }

    private WorkflowDefinitionSummary summarize(JsonNode doc) {
        JsonNode wf = doc.path("mainWorkflow");
        List<String> agentRefs = new ArrayList<>();
        int nodeCount = 0;
        for (JsonNode node : wf.path("nodes")) {
            nodeCount++;
            if ("agent".equals(node.path("type").asText())) {
                agentRefs.add(node.path("agent").path("ref").asText());
            }
        }
        return new WorkflowDefinitionSummary(
                wf.path("id").asText(),
                wf.path("name").asText(),
                wf.path("version").asText(),
                wf.path("status").asText(),
                nodeCount,
                agentRefs);
    }
}
