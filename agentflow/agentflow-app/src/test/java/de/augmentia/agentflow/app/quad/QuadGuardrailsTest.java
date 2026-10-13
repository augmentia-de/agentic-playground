package de.augmentia.agentflow.app.quad;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Named guardrail/hook resolution for declarative agents. */
class QuadGuardrailsTest {

    @Test
    void resolvesKnownGuardrailNamesAndSkipsUnknown() {
        assertThat(QuadGuardrails.fromName("prompt-injection")).isNotNull();
        assertThat(QuadGuardrails.fromName("jailbreak")).isNotNull();
        assertThat(QuadGuardrails.fromName("PII")).isNotNull();
        assertThat(QuadGuardrails.fromName("sql-injection")).isNotNull();
        assertThat(QuadGuardrails.fromName("nope")).isNull();
        assertThat(QuadGuardrails.fromName(null)).isNull();

        assertThat(QuadGuardrails.fromNames(List.of("pii", "unknown", "jailbreak"))).hasSize(2);
        assertThat(QuadGuardrails.fromNames(null)).isEmpty();
    }

    @Test
    void piiAnonymizerFallsBackToDefaultMasks() {
        assertThat(QuadGuardrails.piiAnonymizer(List.of("nope")).name()).isEqualTo("gdpr-pii-anonymizer");
        assertThat(QuadGuardrails.piiAnonymizer(List.of("EMAIL")).name()).isEqualTo("gdpr-pii-anonymizer");
        assertThat(QuadGuardrails.piiAnonymizer(null).name()).isEqualTo("gdpr-pii-anonymizer");
    }

    @Test
    void auditHookIsAvailable() {
        assertThat(QuadGuardrails.auditHook().name()).isEqualTo("gdpr-audit-trail");
    }

    @Test
    void exposesSelectableNames() {
        assertThat(QuadGuardrails.GUARDRAIL_NAMES).contains("pii", "sql-injection", "prompt-injection");
        assertThat(QuadGuardrails.HOOK_NAMES).contains("pii", "audit");
        assertThat(QuadGuardrails.PII_MASK_TYPES).contains("EMAIL", "PHONE_NUMBER", "CREDIT_CARD");
    }
}
