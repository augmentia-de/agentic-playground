package de.augmentia.agentplatform.common.condition;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExpressionEvaluatorTest {

    private static Map<String, Object> ctx(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    @Test
    void numericCompareOnContextPath() {
        Map<String, Object> ctx = ctx("research_step", ctx("confidence", 0.72));
        assertThat(ExpressionEvaluator.eval("context['research_step'].confidence < 0.8", ctx, Map.of())).isTrue();
        ctx = ctx("research_step", ctx("confidence", 0.83));
        assertThat(ExpressionEvaluator.eval("context['research_step'].confidence < 0.8", ctx, Map.of())).isFalse();
        assertThat(ExpressionEvaluator.eval("context['research_step'].confidence >= 0.8", ctx, Map.of())).isTrue();
    }

    @Test
    void stringEquality() {
        Map<String, Object> ctx = ctx("quality", ctx("grade", "A"));
        assertThat(ExpressionEvaluator.eval("context['quality'].grade == 'A'", ctx, Map.of())).isTrue();
        assertThat(ExpressionEvaluator.eval("context['quality'].grade != 'B'", ctx, Map.of())).isTrue();
        assertThat(ExpressionEvaluator.eval("context['quality'].grade == 'B'", ctx, Map.of())).isFalse();
    }

    @Test
    void inputParameterFallback() {
        assertThat(ExpressionEvaluator.eval("input['mode'] == 'fast'", Map.of(), ctx("mode", "fast"))).isTrue();
        assertThat(ExpressionEvaluator.eval("input['mode'] == 'slow'", Map.of(), ctx("mode", "fast"))).isFalse();
    }

    @Test
    void booleanComposition() {
        Map<String, Object> ctx = ctx("a", ctx("x", true), "b", ctx("y", false));
        assertThat(ExpressionEvaluator.eval("context['a'].x == true && context['b'].y == false", ctx, Map.of())).isTrue();
        assertThat(ExpressionEvaluator.eval("context['a'].x == true || context['b'].y == true", ctx, Map.of())).isTrue();
        assertThat(ExpressionEvaluator.eval("!(context['b'].y == true)", ctx, Map.of())).isTrue();
        assertThat(ExpressionEvaluator.eval("(context['a'].x == false && context['b'].y == false) || context['a'].x == true",
                ctx, Map.of())).isTrue();
    }

    @Test
    void missingValuesEvaluateNull() {
        assertThat(ExpressionEvaluator.eval("context['unknown'].field < 0.8", Map.of(), Map.of())).isFalse();
        assertThat(ExpressionEvaluator.eval("context['unknown'].field == null", Map.of(), Map.of())).isTrue();
        assertThat(ExpressionEvaluator.eval("context['unknown'].field != 'x'", Map.of(), Map.of())).isTrue();
    }

    @Test
    void blankExpressionIsTrue() {
        assertThat(ExpressionEvaluator.eval(null, Map.of(), Map.of())).isTrue();
        assertThat(ExpressionEvaluator.eval("   ", Map.of(), Map.of())).isTrue();
    }

    @Test
    void invalidExpressionThrows() {
        assertThatThrownBy(() -> ExpressionEvaluator.eval("context['a' && true", Map.of(), Map.of()))
                .isInstanceOf(ConditionSyntaxException.class);
        assertThatThrownBy(() -> ExpressionEvaluator.eval("><>", Map.of(), Map.of()))
                .isInstanceOf(ConditionSyntaxException.class);
    }
}