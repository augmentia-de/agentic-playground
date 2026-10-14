package de.augmentia.agentflow.app.activities;

import de.augmentia.agentflow.app.config.RequestContext;
import de.augmentia.quad.core.observability.AgentMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.client.ChatClient;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Verifies that every activity binds the per-run context (B12) — the three quad/LangChain4j paths
 * as well as the LangGraph one — and always clears it.
 */
class AgentFlowActivitiesImplTest {

    private static final String RUN_ID = "wf-run-1";

    /** Substitutes the LLM body and records the context/session it observes. */
    static class Capturing extends AgentFlowActivitiesImpl {
        String contextInBody;
        String sessionIdSeen;
        final List<String> calls = new ArrayList<>();

        Capturing() {
            super(mock(ChatClient.Builder.class), mock(AgentMetrics.class));
        }

        @Override
        String runId() {
            return RUN_ID;
        }

        @Override
        String runSimpleAgent(String input, String runId) {
            calls.add("simple");
            contextInBody = RequestContext.current();
            sessionIdSeen = runId;
            return "ok";
        }

        @Override
        String runCapabilities(Set<String> allowedTools, Path skillsDir, List<String> initialSkills,
                               boolean skillSearch, String runId, String input) {
            calls.add("capabilities");
            contextInBody = RequestContext.current();
            sessionIdSeen = runId;
            return "ok";
        }
    }

    @AfterEach
    void clear() {
        RequestContext.clear();
    }

    @Test
    void runQuadAgent_setsRequestContext() {
        Capturing a = new Capturing();
        assertThat(a.runQuadAgent("hi")).isEqualTo("ok");
        assertThat(a.contextInBody).isEqualTo(RUN_ID);
        assertThat(a.sessionIdSeen).isEqualTo(RUN_ID);
    }

    @Test
    void runRichAgent_setsRequestContext() {
        Capturing a = new Capturing();
        assertThat(a.runRichAgent("hi", List.of("readFile"), null)).isEqualTo("ok");
        assertThat(a.contextInBody).isEqualTo(RUN_ID);
        assertThat(a.sessionIdSeen).isEqualTo(RUN_ID);
    }

    @Test
    void runAuthorizedAgent_setsRequestContext() {
        Capturing a = new Capturing();
        assertThat(a.runAuthorizedAgent("hi", List.of("admin"))).isEqualTo("ok");
        assertThat(a.contextInBody).isEqualTo(RUN_ID);
        assertThat(a.sessionIdSeen).isEqualTo(RUN_ID);
    }

    @Test
    void contextIsClearedAfterRun() {
        Capturing a = new Capturing();
        a.runQuadAgent("hi");
        assertThat(RequestContext.current()).isNull();

        // A second run must not see a stale value from the first.
        a.contextInBody = null;
        a.runRichAgent("hi", List.of(), null);
        assertThat(a.contextInBody).isEqualTo(RUN_ID);
        assertThat(RequestContext.current()).isNull();
    }

    @Test
    void contextIsClearedWhenBodyThrows() {
        Capturing a = new Capturing() {
            @Override
            String runSimpleAgent(String input, String runId) {
                throw new IllegalStateException("boom");
            }
        };
        assertThatThrownBy(() -> a.runQuadAgent("hi")).isInstanceOf(IllegalStateException.class);
        assertThat(RequestContext.current()).isNull();
    }
}
