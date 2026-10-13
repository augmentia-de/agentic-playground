package de.augmentia.agentflow.examples.research;

import de.augmentia.agentflow.examples.Examples;
import de.augmentia.quad.core.agent.Agent;
import de.augmentia.quad.core.agent.AgentBuilder;
import de.augmentia.quad.core.config.ModelFactory;
import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.tool.builtin.WebFetchTool;
import de.augmentia.quad.core.tool.builtin.WebSearchTool;
import org.springframework.stereotype.Component;
import io.temporal.spring.boot.ActivityImpl;

/** Market research: a research sub-agent plus a writer agent. */
@Component
@ActivityImpl(taskQueues = "agentflow")
public class ResearchActivitiesImpl implements ResearchActivities {

    @Override
    public String research(String topic) {
        Agent researcher = Examples.agent(
                "You are a research sub-agent. Search the web and gather concrete, sourced facts.",
                new WebSearchTool(), new WebFetchTool());
        Agent orchestrator = AgentBuilder.defaultAgent()
                .withModel(ModelFactory.createOpenAiFromEnv())
                .withSubAgent("researcher", researcher)
                .withSystemPrompt("Delegate to the 'researcher' tool when you need facts, "
                        + "then summarise the findings in bullet points.")
                .build();
        return orchestrator.run(topic, new AgentSessionState());
    }

    @Override
    public String writeBrief(String research) {
        Agent writer = Examples.agent(
                "You are a business analyst. Turn research notes into a concise executive brief "
                        + "with a short summary, three key findings and a recommendation.");
        return writer.run("Research notes:\n" + research, new AgentSessionState());
    }
}
