package de.augmentia.agentflow.examples.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.quad.core.agent.Agent;
import de.augmentia.quad.core.agent.AgentBuilder;
import de.augmentia.quad.core.agent.AgentResult;
import de.augmentia.quad.core.capability.Capability;
import de.augmentia.quad.core.capability.CapabilitySearch;
import de.augmentia.quad.core.config.ModelFactory;
import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.memory.MemoryCategory;
import de.augmentia.quad.core.session.memory.MemoryEntry;
import de.augmentia.quad.core.session.memory.PersistentMemoryStore;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import io.temporal.spring.boot.ActivityImpl;

/** Support triage: structured classification + memory + capability/RAG lookup. */
@Component
@ActivityImpl(taskQueues = "agentflow")
public class SupportActivitiesImpl implements SupportActivities {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PersistentMemoryStore memory;
    private final CapabilitySearch capabilitySearch;

    public SupportActivitiesImpl(PersistentMemoryStore memory, CapabilitySearch capabilitySearch) {
        this.memory = memory;
        this.capabilitySearch = capabilitySearch;
    }

    @Override
    public String assessTicket(String message) {
        Agent agent = AgentBuilder.defaultAgent()
                .withModel(ModelFactory.createOpenAiFromEnv())
                .withSystemPrompt("You classify customer support tickets. "
                        + "Return category, priority (LOW/MEDIUM/HIGH), sentiment and a short suggested reply.")
                .withStructuredOutput(TicketAssessment.class)
                .build();
        AgentResult result = agent.executeStructured(message, new AgentSessionState());
        return result.structuredOutput() != null ? result.structuredOutput() : result.finalAnswer();
    }

    @Override
    public String recallCustomer(String customerId) {
        return memory.formatForInjection(customerId);
    }

    @Override
    public String rememberTicket(String customerId, String summary) {
        String id = "ticket-" + UUID.randomUUID();
        memory.remember(new MemoryEntry(id, customerId, summary, summary,
                MemoryCategory.PROJECT_FACT, Instant.now(), false));
        return id;
    }

    @Override
    public String findPlaybook(String topic) {
        List<Capability> matches = capabilitySearch.search(topic, 3);
        return matches.stream().map(Capability::name).collect(Collectors.joining(","));
    }
}
