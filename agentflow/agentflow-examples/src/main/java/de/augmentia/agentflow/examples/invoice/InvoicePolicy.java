package de.augmentia.agentflow.examples.invoice;

import java.util.List;

/** Deterministic approval rules for invoices. */
public final class InvoicePolicy {

    /** Amounts above this need a second approval and are handled by the saga. */
    public static final double DUAL_APPROVAL_THRESHOLD = 100.0;

    private InvoicePolicy() {
    }

    public static boolean requiresDualApproval(double amount) {
        return amount > DUAL_APPROVAL_THRESHOLD;
    }

    public static List<String> approvalChain(double amount) {
        return requiresDualApproval(amount)
                ? List.of("team-lead", "finance")
                : List.of("finance");
    }
}
