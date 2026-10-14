package de.augmentia.quad.core.hook.pipeline;

import de.augmentia.quad.core.guardrails.GuardrailException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** Hook failures must follow the configured policy instead of being silently swallowed. */
class HookRegistryTest {

    private static HookContexts.BeforeAgentContext ctx() {
        return new HookContexts.BeforeAgentContext("s", "hi", Map.of());
    }

    private static AgentHook throwing(String name, RuntimeException error) {
        return new AgentHook() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public HookResult beforeAgent(HookContexts.BeforeAgentContext ctx) {
                throw error;
            }
        };
    }

    @Test
    void wrap_propagatesOrLogsPerPolicy() {
        // ISOLATE (default): the failing hook is skipped, the chain continues.
        var isolate = new HookRegistry();
        isolate.register(throwing("a", new IllegalStateException("boom-a")));
        var result = isolate.triggerBeforeAgent(ctx());
        assertInstanceOf(HookResult.Modify.class, result);

        // CHAIN_ABORT: the failure propagates to the caller.
        var abort = new HookRegistry();
        abort.setFailurePolicy(HookFailurePolicy.CHAIN_ABORT);
        abort.register(throwing("b", new IllegalStateException("boom-b")));
        assertThatThrownBy(() -> abort.triggerBeforeAgent(ctx()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("boom-b");
    }

    @Test
    void guardrailControlFlowIsNeverSwallowed() {
        var registry = new HookRegistry(); // ISOLATE — must still propagate guardrail signals
        registry.register(throwing("guard", new GuardrailException("blocked")));

        assertThatThrownBy(() -> registry.triggerBeforeAgent(ctx()))
                .isInstanceOf(GuardrailException.class);
    }

    @Test
    void failurePolicyVisibleAcrossThreads() throws Exception {
        Field field = HookRegistry.class.getDeclaredField("failurePolicy");
        assertThat(Modifier.isVolatile(field.getModifiers()))
                .as("failurePolicy must be volatile so a change is visible to other threads")
                .isTrue();
    }
}
