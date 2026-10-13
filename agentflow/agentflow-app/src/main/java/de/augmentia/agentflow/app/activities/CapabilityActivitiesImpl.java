package de.augmentia.agentflow.app.activities;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.agentflow.app.capability.CapabilityCatalog;
import de.augmentia.agentflow.app.quad.QuadCapabilities;
import de.augmentia.agentflow.app.workflow.CapabilityActivities;
import de.augmentia.quad.core.agent.Agent;
import de.augmentia.quad.core.agent.AgentBuilder;
import de.augmentia.quad.core.capability.Capability;
import de.augmentia.quad.core.capability.CapabilitySearch;
import de.augmentia.quad.core.config.ModelFactory;
import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.CurrentSession;
import de.augmentia.quad.core.session.WorkspaceResolver;
import de.augmentia.quad.core.tool.CapabilitySearchTool;
import de.augmentia.quad.core.tool.QuadToolRegistry;
import dev.langchain4j.model.chat.ChatModel;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import io.temporal.spring.boot.ActivityImpl;

/** Capability search / RAG activities backed by quad-core. */
@Component
@ActivityImpl(taskQueues = "agentflow")
public class CapabilityActivitiesImpl implements CapabilityActivities {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final CapabilitySearch capabilitySearch;

    public CapabilityActivitiesImpl(CapabilitySearch capabilitySearch) {
        this.capabilitySearch = capabilitySearch;
    }

    @Override
    public String searchCapabilities(String query, int topK, String tenant) {
        int k = topK > 0 ? topK : 10;
        String effectiveTenant = (tenant == null || tenant.isBlank())
                ? de.augmentia.agentflow.app.security.TenantContext.current()
                : tenant;
        List<Capability> results = (effectiveTenant == null || effectiveTenant.isBlank())
                ? capabilitySearch.search(query, k)
                : capabilitySearch.search(query, k, effectiveTenant);
        try {
            return MAPPER.writeValueAsString(results);
        } catch (Exception e) {
            return "[]";
        }
    }

    @Override
    public String runWithCapabilitySearch(String input) {
        ChatModel model = ModelFactory.createOpenAiFromEnv();
        WorkspaceResolver resolver = new WorkspaceResolver();

        QuadToolRegistry registry = QuadCapabilities.toolRegistry(Set.of(), resolver);
        CapabilitySearchTool tool = new CapabilitySearchTool(capabilitySearch, model, null);
        registry.register(tool.spec().name(), tool);

        Agent agent = AgentBuilder.defaultAgent()
                .withModel(model)
                .withToolRegistry(registry)
                .withWorkspaceResolver(resolver)
                .withCurrentSession(new CurrentSession())
                .withGuardrail(QuadCapabilities.promptInjectionGuardrail())
                .withLogging()
                .build();

        return agent.run(input, new AgentSessionState());
    }

    /** Exposed for tests/UI: the indexed catalog. */
    public static List<Capability> catalog() {
        return CapabilityCatalog.all();
    }
}
