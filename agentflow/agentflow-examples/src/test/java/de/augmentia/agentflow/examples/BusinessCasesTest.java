package de.augmentia.agentflow.examples;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BusinessCasesTest {

    @Test
    void shipsFiveDistinctCases() {
        assertThat(BusinessCases.all()).hasSize(5);
        assertThat(BusinessCases.all()).extracting(BusinessCases.Case::id)
                .containsExactlyInAnyOrder("support-triage", "contract-review", "invoice-processing",
                        "market-research", "onboarding");
    }
}
