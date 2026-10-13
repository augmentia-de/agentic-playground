package de.augmentia.agentflow.examples.invoice;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Forward steps and their compensations for the invoice processing saga. Compensations are
 * the reverse of the forward steps; the workflow decides when to run them.
 */
@ActivityInterface
public interface InvoiceActivities {

    /** Extracts a {@link ParsedInvoice} from free text (structured output). */
    @ActivityMethod
    String extractInvoice(String text);

    // forward
    @ActivityMethod
    String reserveBudget(String invoiceId);

    @ActivityMethod
    String postLedger(String invoiceId);

    /** Fails when the amount exceeds the policy threshold (drives compensation). */
    @ActivityMethod
    String schedulePayment(String invoiceId, double amount);

    // compensation (reverse order)
    @ActivityMethod
    void reverseLedger(String invoiceId);

    @ActivityMethod
    void releaseBudget(String reservationId);
}
