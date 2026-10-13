package de.augmentia.agentflow.app.quad;

import de.augmentia.agentflow.app.activities.SagaActivitiesImpl;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QuadSecurityAndSagaTest {

    @Test
    void readerCannotWriteOrSearchWeb() {
        Set<String> allowed = QuadSecurity.allowedTools(Set.of("reader"));
        assertThat(allowed).contains("readFile", "listDirectory", "grepSearch", "findFiles");
        assertThat(allowed).doesNotContain("writeFile", "webSearch", "webfetch");
    }

    @Test
    void writerCanWriteButNotSearch() {
        Set<String> allowed = QuadSecurity.allowedTools(Set.of("writer"));
        assertThat(allowed).contains("writeFile", "readFile");
        assertThat(allowed).doesNotContain("webSearch");
    }

    @Test
    void researcherCanSearchWeb() {
        assertThat(QuadSecurity.allowedTools(Set.of("researcher")))
                .contains("webSearch", "webfetch", "readFile")
                .doesNotContain("writeFile");
    }

    @Test
    void noRolesGetNoTools() {
        assertThat(QuadSecurity.allowedTools(Set.of())).isEmpty();
    }

    @Test
    void quadSagaUnwindsCompensations() {
        String outcome = new SagaActivitiesImpl().quadSaga("x");
        assertThat(outcome).contains("compensated: reserve").contains("compensated: charge");
    }

    @Test
    void chargeFailsAboveThreshold() {
        assertThatThrownBy(() -> new SagaActivitiesImpl().charge("order-1", 200.0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("payment declined");
    }
}
