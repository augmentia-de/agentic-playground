package de.augmentia.agentflow.app.workflow.declarative;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConditionEvaluatorTest {

    @Test
    void blankIsAlwaysTrue() {
        assertThat(ConditionEvaluator.matches(null, "anything")).isTrue();
        assertThat(ConditionEvaluator.matches("  ", "anything")).isTrue();
    }

    @Test
    void containsAndNotContains() {
        assertThat(ConditionEvaluator.matches("contains:expensive", "This is EXPENSIVE")).isTrue();
        assertThat(ConditionEvaluator.matches("notcontains:cheap", "expensive item")).isTrue();
        assertThat(ConditionEvaluator.matches("notcontains:expensive", "expensive item")).isFalse();
    }

    @Test
    void startsEqualsTruthyFalsy() {
        assertThat(ConditionEvaluator.matches("starts:DONE", "  done all")).isTrue();
        assertThat(ConditionEvaluator.matches("equals:ok", " OK ")).isTrue();
        assertThat(ConditionEvaluator.matches("truthy", "approved=true")).isTrue();
        assertThat(ConditionEvaluator.matches("falsy", "")).isTrue();
    }

    @Test
    void unknownOperatorIsFalse() {
        assertThat(ConditionEvaluator.matches("regex:.*", "x")).isFalse();
    }
}
