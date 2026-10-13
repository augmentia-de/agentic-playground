package de.augmentia.agentflow.app.web;

import de.augmentia.agentflow.app.config.AgentFlowLlmHeaderProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Effective configuration for the settings screen. Never exposes secrets: the API key is
 * reduced to a boolean ({@code hasApiKey}), header <i>names</i> only (never values).
 */
@RestController
public class SettingsController {

    @Value("${spring.ai.openai.chat.options.model:}")
    private String llmModel;
    @Value("${spring.ai.openai.base-url:}")
    private String llmBaseUrl;
    @Value("${spring.ai.openai.api-key:}")
    private String apiKey;

    private final AgentFlowLlmHeaderProperties llmHeaders;

    public SettingsController(AgentFlowLlmHeaderProperties llmHeaders) {
        this.llmHeaders = llmHeaders;
    }

    @Value("${spring.temporal.connection.target:localhost:7233}")
    private String temporalTarget;
    @Value("${spring.temporal.namespace:default}")
    private String temporalNamespace;

    @Value("${agentflow.capability.index:hnsw}")
    private String capabilityIndex;
    @Value("${agentflow.capability.table:capability_index}")
    private String capabilityTable;
    @Value("${agentflow.capability.autoload:true}")
    private boolean capabilityAutoload;

    @Value("${agentflow.workflows.location:classpath*:workflows/*.json}")
    private String workflowsLocation;
    @Value("${agentflow.workflows.state-dir:target/agentflow-workflow-overrides}")
    private String workflowsStateDir;
    @Value("${agentflow.checkpoint.path:target/agentflow-checkpoints.db}")
    private String checkpointPath;
    @Value("${agentflow.ui.temporal-url:http://localhost:8233}")
    private String temporalUiUrl;

    @GetMapping("/agentflow/settings")
    public Map<String, Object> settings() {
        Map<String, Object> llm = new LinkedHashMap<>();
        llm.put("model", llmModel);
        llm.put("baseUrl", llmBaseUrl);
        llm.put("hasApiKey", apiKey != null && !apiKey.isBlank());
        llm.put("headersConfigured", llmHeaders.isConfigured());
        List<String> headerNames = new ArrayList<>(llmHeaders.getHeaders().keySet());
        llm.put("headerNames", headerNames);

        Map<String, Object> temporal = new LinkedHashMap<>();
        temporal.put("target", temporalTarget);
        temporal.put("namespace", temporalNamespace);
        temporal.put("taskQueue", "agentflow");

        Map<String, Object> capability = new LinkedHashMap<>();
        capability.put("index", capabilityIndex);
        capability.put("table", capabilityTable);
        capability.put("autoload", capabilityAutoload);

        Map<String, Object> workflows = new LinkedHashMap<>();
        workflows.put("location", workflowsLocation);
        workflows.put("stateDir", workflowsStateDir);

        Map<String, Object> endpoints = new LinkedHashMap<>();
        endpoints.put("metrics", "/actuator/prometheus");
        endpoints.put("health", "/actuator/health");
        endpoints.put("temporalUi", temporalUiUrl);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("llm", llm);
        out.put("temporal", temporal);
        out.put("capability", capability);
        out.put("workflows", workflows);
        out.put("checkpoint", Map.of("path", checkpointPath));
        out.put("endpoints", endpoints);
        return out;
    }
}
