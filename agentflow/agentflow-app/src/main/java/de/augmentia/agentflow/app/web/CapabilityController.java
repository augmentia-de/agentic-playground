package de.augmentia.agentflow.app.web;

import de.augmentia.agentflow.app.capability.CapabilityCatalog;
import de.augmentia.agentflow.app.security.TenantContext;
import de.augmentia.agentflow.app.workflow.CapabilitySearchWorkflow;
import de.augmentia.quad.core.capability.Capability;
import de.augmentia.quad.core.capability.CapabilitySearch;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** REST access to the capability catalog and retrieval (RAG over tools/skills). */
@RestController
public class CapabilityController {

    static final String TASK_QUEUE = "agentflow";

    private final WorkflowClient workflowClient;
    private final CapabilitySearch capabilitySearch;

    public CapabilityController(WorkflowClient workflowClient, CapabilitySearch capabilitySearch) {
        this.workflowClient = workflowClient;
        this.capabilitySearch = capabilitySearch;
    }

    /** Durable capability search + agent run that can discover tools at runtime. */
    @PostMapping("/agentflow/runs/capability-search")
    public Map<String, Object> start(@RequestParam("input") String input,
                                     @RequestParam(name = "query", required = false) String query,
                                     @RequestParam(name = "topK", defaultValue = "10") int topK,
                                     @RequestParam(name = "tenant", required = false) String tenant) {
        String effectiveQuery = (query == null || query.isBlank()) ? input : query;
        String workflowId = "agentflow-cap-" + UUID.randomUUID();
        CapabilitySearchWorkflow workflow = workflowClient.newWorkflowStub(
                CapabilitySearchWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build());
        WorkflowClient.start(workflow::run, input, effectiveQuery, topK, tenant);
        return Map.of("workflowId", workflowId, "started", true, "query", effectiveQuery);
    }

    /** The static capability catalog (what the index is seeded with). */
    @GetMapping("/agentflow/capabilities")
    public List<Capability> catalog() {
        return CapabilityCatalog.all();
    }

    /** Synchronous retrieval, e.g. for the UI config/search screen. */
    @GetMapping("/agentflow/capabilities/search")
    public List<Map<String, Object>> search(@RequestParam("q") String q,
                                            @RequestParam(name = "topK", defaultValue = "10") int topK,
                                            @RequestParam(name = "tenant", required = false) String tenant) {
        String effectiveTenant = (tenant == null || tenant.isBlank()) ? TenantContext.current() : tenant;
        List<Capability> results = (effectiveTenant == null || effectiveTenant.isBlank())
                ? capabilitySearch.search(q, topK)
                : capabilitySearch.search(q, topK, effectiveTenant);
        return results.stream().map(c -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", c.name());
            m.put("description", c.description());
            m.put("type", c.type());
            m.put("source", c.source());
            return m;
        }).toList();
    }
}
