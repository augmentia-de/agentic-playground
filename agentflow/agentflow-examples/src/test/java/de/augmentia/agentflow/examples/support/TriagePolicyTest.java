package de.augmentia.agentflow.examples.support;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TriagePolicyTest {

    @Test
    void escalatesHighPriority() {
        assertThat(TriagePolicy.requiresEscalation("{\"priority\":\"HIGH\",\"category\":\"billing\"}")).isTrue();
        assertThat(TriagePolicy.requiresEscalation("{ \"priority\" : \"high\" }")).isTrue();
    }

    @Test
    void doesNotEscalateLowerPriorities() {
        assertThat(TriagePolicy.requiresEscalation("{\"priority\":\"MEDIUM\"}")).isFalse();
        assertThat(TriagePolicy.requiresEscalation("{\"priority\":\"LOW\"}")).isFalse();
        assertThat(TriagePolicy.requiresEscalation(null)).isFalse();
    }
}
