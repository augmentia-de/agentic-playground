package de.augmentia.agentflow.examples.support;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;
import io.temporal.spring.boot.WorkflowImpl;

/**
 * Business case 1 — Customer support triage.
 *
 * <p>Classifies a ticket (structured output), augments it with the customer's memory and a
 * capability/RAG lookup, and — for HIGH priority — pauses durably for a human escalation
 * decision (Temporal signal).</p>
 */
@WorkflowInterface
public interface SupportTriageWorkflow {

    @WorkflowMethod
    String run(String customerId, String message);

    /** Human escalation decision for HIGH-priority tickets. */
    @SignalMethod
    void escalate(boolean approved);

    @WorkflowImpl(taskQueues = "agentflow")
    class Impl implements SupportTriageWorkflow {
        private final SupportActivities activities = Workflow.newActivityStub(
                SupportActivities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofMinutes(5))
                        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(2).build())
                        .build());

        private Boolean escalationApproved;

        @Override
        public String run(String customerId, String message) {
            String memory = activities.recallCustomer(customerId);
            String assessment = activities.assessTicket(message);
            String playbook = activities.findPlaybook(message);
            activities.rememberTicket(customerId, "ticket: " + message);

            if (TriagePolicy.requiresEscalation(assessment)) {
                Workflow.await(() -> escalationApproved != null);
                return "escalated=" + escalationApproved
                        + " assessment=" + assessment
                        + " playbook=" + playbook;
            }
            return "handled assessment=" + assessment
                    + " memory=" + memory
                    + " playbook=" + playbook;
        }

        @Override
        public void escalate(boolean approved) {
            this.escalationApproved = approved;
        }
    }
}
