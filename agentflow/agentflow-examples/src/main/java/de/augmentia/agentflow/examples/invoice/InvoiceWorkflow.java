package de.augmentia.agentflow.examples.invoice;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.time.Duration;
import io.temporal.spring.boot.WorkflowImpl;

/**
 * Business case 3 — Invoice processing as a durable saga.
 *
 * <p>Forward: reserve budget → post ledger → schedule payment. If payment fails, the
 * workflow compensates in reverse order (reverse ledger, release budget). The compensation
 * is deterministic workflow code, so it survives worker restarts.</p>
 */
@WorkflowInterface
public interface InvoiceWorkflow {

    @WorkflowMethod
    String run(String invoiceId, double amount);

    @WorkflowImpl(taskQueues = "agentflow")
    class Impl implements InvoiceWorkflow {
        private final InvoiceActivities activities = Workflow.newActivityStub(
                InvoiceActivities.class,
                ActivityOptions.newBuilder()
                        .setStartToCloseTimeout(Duration.ofSeconds(30))
                        // Single attempt so a business failure surfaces deterministically.
                        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(1).build())
                        .build());

        @Override
        public String run(String invoiceId, double amount) {
            String reservation = activities.reserveBudget(invoiceId);
            try {
                activities.postLedger(invoiceId);
                return "paid: " + activities.schedulePayment(invoiceId, amount);
            } catch (ActivityFailure failure) {
                activities.reverseLedger(invoiceId);
                activities.releaseBudget(reservation);
                return "compensated: released=" + reservation;
            }
        }
    }
}
