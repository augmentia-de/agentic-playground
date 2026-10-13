package de.augmentia.agentflow.examples.contract;

import de.augmentia.agentflow.examples.Examples;
import de.augmentia.quad.core.agent.Agent;
import de.augmentia.quad.core.agent.AgentBuilder;
import de.augmentia.quad.core.agent.AgentResult;
import de.augmentia.quad.core.config.ModelFactory;
import de.augmentia.quad.core.session.AgentSessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import io.temporal.spring.boot.ActivityImpl;

/** Contract review with PII anonymization and a prompt-injection guardrail. */
@Component
@ActivityImpl(taskQueues = "agentflow")
public class ContractActivitiesImpl implements ContractActivities {

    private static final Logger log = LoggerFactory.getLogger(ContractActivitiesImpl.class);

    @Override
    public String analyzeContract(String contractText) {
        Agent agent = AgentBuilder.defaultAgent()
                .withModel(ModelFactory.createOpenAiFromEnv())
                .withSystemPrompt("You are a contract reviewer. Identify risky clauses and give a "
                        + "risk level (LOW/MEDIUM/HIGH) plus a recommendation.")
                .withGuardrail(Examples.promptInjectionGuardrail())
                .withHook(Examples.piiHook())
                .withStructuredOutput(ContractReview.class)
                .build();
        AgentResult result = agent.executeStructured(contractText, new AgentSessionState());
        return result.structuredOutput() != null ? result.structuredOutput() : result.finalAnswer();
    }

    @Override
    public String recordDecision(String summary) {
        log.info("[contract] recorded decision: {}", summary);
        return "recorded";
    }
}
