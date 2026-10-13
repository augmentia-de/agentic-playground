package de.augmentia.agentflow.examples.invoice;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InvoicePolicyTest {

    @Test
    void amountsAboveThresholdNeedDualApproval() {
        assertThat(InvoicePolicy.requiresDualApproval(100.0)).isFalse();
        assertThat(InvoicePolicy.requiresDualApproval(100.01)).isTrue();
        assertThat(InvoicePolicy.approvalChain(500.0)).containsExactly("team-lead", "finance");
        assertThat(InvoicePolicy.approvalChain(50.0)).containsExactly("finance");
    }
}
