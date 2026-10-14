package de.augmentia.quad.core.session;

import de.augmentia.quad.core.session.memory.SessionMemory;
import dev.langchain4j.data.message.UserMessage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** A snapshot round-trip must not lose the compacted conversation summary. */
class SessionStateSnapshotTest {

    @Test
    void roundTrip_preservesSummary() {
        var state = AgentSessionState.create("s1");
        var memory = new SessionMemory(2);
        memory.record(List.of(
                UserMessage.from("m1"), UserMessage.from("m2"), UserMessage.from("m3")));
        assertThat(memory.summary()).as("precondition: compaction produced a summary").isNotNull();
        state.setMemory(memory);

        AgentSessionState restored = SessionStateSnapshot.from(state).toState();

        assertThat(restored.memory().summary()).isEqualTo(memory.summary());
        assertThat(restored.memory().maxMessages()).isEqualTo(memory.maxMessages());
    }

    @Test
    void roundTrip_preservesNullSummary() {
        var state = AgentSessionState.create("s1");
        state.setMemory(new SessionMemory(5));

        AgentSessionState restored = SessionStateSnapshot.from(state).toState();

        assertThat(restored.memory().summary()).isNull();
    }
}
