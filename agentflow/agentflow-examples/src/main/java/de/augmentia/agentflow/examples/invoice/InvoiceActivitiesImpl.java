package de.augmentia.agentflow.examples.invoice;

import de.augmentia.quad.core.agent.Agent;
import de.augmentia.quad.core.agent.AgentBuilder;
import de.augmentia.quad.core.agent.AgentResult;
import de.augmentia.quad.core.config.ModelFactory;
import de.augmentia.quad.core.session.AgentSessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import io.temporal.spring.boot.ActivityImpl;

/** Side-effecting invoice steps (would call real ERP/payment systems in production). */
@Component
@ActivityImpl(taskQueues = "agentflow")
public class InvoiceActivitiesImpl implements InvoiceActivities {

    private static final Logger log = LoggerFactory.getLogger(InvoiceActivitiesImpl.class);

    @Override
    public String extractInvoice(String text) {
        Agent agent = AgentBuilder.defaultAgent()
                .withModel(ModelFactory.createOpenAiFromEnv())
                .withSystemPrompt("Extract the invoice id, total, currency and vendor from the text.")
                .withStructuredOutput(ParsedInvoice.class)
                .build();
        AgentResult result = agent.executeStructured(text, new AgentSessionState());
        return result.structuredOutput() != null ? result.structuredOutput() : result.finalAnswer();
    }

    @Override
    public String reserveBudget(String invoiceId) {
        log.info("[invoice] reserve budget for {}", invoiceId);
        return "res-" + invoiceId;
    }

    @Override
    public String postLedger(String invoiceId) {
        log.info("[invoice] post ledger for {}", invoiceId);
        return "posted:" + invoiceId;
    }

    @Override
    public String schedulePayment(String invoiceId, double amount) {
        if (amount > InvoicePolicy.DUAL_APPROVAL_THRESHOLD) {
            throw new IllegalStateException("payment declined for invoice " + invoiceId + " (amount=" + amount + ")");
        }
        log.info("[invoice] schedule payment for {} amount={}", invoiceId, amount);
        return "scheduled:" + invoiceId;
    }

    @Override
    public void reverseLedger(String invoiceId) {
        log.warn("[invoice] COMPENSATE reverse ledger for {}", invoiceId);
    }

    @Override
    public void releaseBudget(String reservationId) {
        log.warn("[invoice] COMPENSATE release budget {}", reservationId);
    }
}
