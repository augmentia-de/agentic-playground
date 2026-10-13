package de.augmentia.quad.worker.tool;

import de.augmentia.quad.core.annotation.Tool;
import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.CurrentSession;
import de.augmentia.quad.core.tool.QuadToolRegistry;
import de.augmentia.quad.core.tool.ReflectiveToolMethod;
import de.augmentia.quad.core.tool.ToolArgsMapper;
import de.augmentia.quad.core.tool.ToolMethod;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression guard for the quad integration of this worker.
 *
 * <p>These tests are not tests of the worker but guards over the coupling to
 * {@code quad-core}. They cover the two places where a correction in quad
 * (TODO2.md) would break the worker:
 *
 * <ul>
 *   <li><b>A1</b> — the worker builds its own classes with {@code -parameters}
 *       ({@code quad-workflow-worker/pom.xml:194}), quad itself does not. A tool <em>without</em>
 *       {@code @Param} still has to bind here.</li>
 *   <li><b>A7</b> — all seven registered builtins read the session state via
 *       {@code currentState()} → {@link CurrentSession}. As long as quad keeps the ThreadLocal as
 *       fallback, this path has to work.</li>
 * </ul>
 */
class QuadIntegrationContractTest {

    private static ToolArgsMapper mapper() {
        return new ToolArgsMapper(new com.fasterxml.jackson.databind.ObjectMapper());
    }

    /** Tool without {@code @Param} — corresponds to JdbcAgentConfigSourceE2ETest:163-172. */
    public static class NoParamTool {
        @Tool("Read a file from workspace")
        public String readFile(String path) {
            return "content of " + path;
        }
    }

    /** Tool with {@code @Param} and a session state parameter. */
    public static class StatefulTool {
        @Tool("reports the session")
        public String report(AgentSessionState state,
                             @de.augmentia.quad.core.annotation.Param("topic") String topic) {
            return topic + "@" + (state == null ? "null" : state.getSessionId());
        }
    }

    // ── A1: the -parameters boundary ───────────────────────────────────────

    @Test
    void toolWithoutParamAnnotation_bindsByRealParameterName() throws Exception {
        var tm = new ReflectiveToolMethod(
                NoParamTool.class.getMethod("readFile", String.class), new NoParamTool(), mapper());

        assertThat(tm.spec().parameters().properties())
                .as("TODO2 A1: the worker builds with -parameters, so getName() must return the "
                        + "real names. If that falls back to 'arg0', the "
                        + "worker loses every argument.")
                .containsKey("path");

        assertThat(tm.execute("{\"path\":\"a.txt\"}", null).text()).isEqualTo("content of a.txt");
    }

    @Test
    void toolWithoutParamAnnotation_fallsBackToArgZeroWhenParametersAreMissing() throws Exception {
        var tm = new ReflectiveToolMethod(
                NoParamTool.class.getMethod("readFile", String.class), new NoParamTool(), mapper());

        var props = tm.spec().parameters().properties();
        var name = props.containsKey("path") ? "path" : "arg0";

        // Documents the behaviour in both build variants so that the difference
        // sichtbar bleibt statt zu ueberraschen.
        assertThat(tm.execute("{\"" + name + "\":\"a.txt\"}", null).text())
                .isEqualTo("content of a.txt");
    }

    // ── A7: ThreadLocal als Fallback ─────────────────────────────────────────

    @Test
    void sessionStateParameter_isFilledFromTheExecuteArgument() throws Exception {
        var tm = new ReflectiveToolMethod(
                StatefulTool.class.getMethod("report", AgentSessionState.class, String.class),
                new StatefulTool(), mapper());

        CurrentSession.setCurrent(null);
        var argumentState = AgentSessionState.create("from-argument");

        assertThat(tm.execute("{\"topic\":\"bugs\"}", argumentState).text())
                .as("TODO2 A7: ReflectiveToolMethod.execute:121 sets the ThreadLocal from the "
                        + "argument and buildArgs:143 reads it back. The argument state must "
                        + "arrive at the tool.")
                .isEqualTo("bugs@from-argument");
    }

    @Test
    void executeWithNullStateKeepsTheExistingThreadLocalBinding() throws Exception {
        var tm = new ReflectiveToolMethod(
                StatefulTool.class.getMethod("report", AgentSessionState.class, String.class),
                new StatefulTool(), mapper());

        // The worker binds the session itself (TaskExecutor:216). A caller without
        // state view must not destroy this state.
        CurrentSession.setCurrent(AgentSessionState.create("worker-session"));
        try {
            var text = tm.execute("{\"topic\":\"bugs\"}", null).text();

            assertThat(text)
                    .as("TODO2 A7 fixed: execute() saves the ThreadLocal and restores it "
                            + "again, even if state is null")
                    .isEqualTo("bugs@worker-session");
        } finally {
            CurrentSession.setCurrent(null);
        }
    }

    // ── Registry contract that the worker uses ────────────────────────────

    @Test
    void agentToolRegistryFactory_stillBuildsARegistryWithAgentAndGlobalTools() throws Exception {
        var catalog = new ToolCatalog(mapper(), Set.of(new NoParamTool()));
        var factory = new AgentToolRegistryFactory(catalog, mapper());

        var registry = factory.create(new de.augmentia.quad.core.agent.Agent() {
            @Override
            protected AgentSessionState newSessionState() {
                return new AgentSessionState();
            }
        }, Set.of("readFile"));

        assertThat(registry.get("readFile"))
                .as("TODO2 A10: AgentToolRegistryFactory:28-37 uses registerFromAgent + register; "
                        + "both must remain valid after the registry hardening.")
                .isNotNull();
    }

    @Test
    void unknownGlobalTool_throwsFromTheCatalogDuringRegistryConstruction() {
        var catalog = new ToolCatalog(mapper(), Set.of(new NoParamTool()));
        var factory = new AgentToolRegistryFactory(catalog, mapper());
        var agent = new de.augmentia.quad.core.agent.Agent() {
            @Override
            protected AgentSessionState newSessionState() {
                return new AgentSessionState();
            }
        };

        // Der Katalog wirft fail-fast (ToolCatalog:48-52), die Registry gibt null
        // back (TODO2 A10). The error therefore comes BEFORE the registry.
        assertThatThrownBy(() -> factory.create(agent, Set.of("doesNotExist")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("doesNotExist");
    }

    @Test
    void registryReturnsNullForAnUnregisteredName() {
        var reg = new QuadToolRegistry(mapper());

        assertThat(reg.get("doesNotExist"))
                .as("TODO2 A10: QuadToolRegistry.get returns null; strands throws. "
                        + "Every caller needs a null check, and everyone can forget it.")
                .isNull();
    }

    @Test
    void registryRemainsUsableAfterWithOnlyAndRemove() {
        var reg = new QuadToolRegistry(mapper());
        var stub = new ToolMethod() {
            @Override
            public dev.langchain4j.agent.tool.ToolSpecification spec() {
                return dev.langchain4j.agent.tool.ToolSpecification.builder()
                        .name("alpha").description("stub").build();
            }

            @Override
            public de.augmentia.quad.core.tool.ToolResult execute(String jsonArguments, AgentSessionState state) {
                return de.augmentia.quad.core.tool.ToolResult.success("A");
            }
        };
        reg.register("alpha", stub);

        assertThat(reg.withOnly(Set.of("alpha")).get("alpha")).isNotNull();
        reg.remove("alpha");
        assertThat(reg.get("alpha")).isNull();
    }
}
