package de.augmentia.agentplatform.orchestrator.kafka;

import de.augmentia.agentplatform.bridge.redis.EmailInitiatorStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.Acknowledgment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Intake tracking and config update listener.
 *
 * <p>Both classes lay completely outside the test coverage. The
 * ConfigUpdateConsumer has an error case that stands out: the listener
 * acknowledges regardless of the result, which on a usable but internally
 * failed update prevents the retry.
 */
@DisplayName("Intake and config listener")
class IntakeAndConfigListenerTest {

    // ── IntakeListener.trackEmailIntake ──────────────────────────────

    @Test
    @DisplayName("Email intake stores initiator and dedup entry")
    void emailIntakeStoresInitiatorAndDedup() {
        EmailInitiatorStore store = mock(EmailInitiatorStore.class);
        var listener = new IntakeListener(mock(org.springframework.kafka.core.KafkaTemplate.class), store);

        listener.trackEmailIntake("""
                {"workflowId":"wf-1","initiatorEmail":"a@example.com","messageId":"m-1"}
                """);

        InOrder order = inOrder(store);
        order.verify(store).storeInitiator("wf-1", "a@example.com");
        order.verify(store).registerIntake("m-1", "wf-1");
    }

    @Test
    @DisplayName("Without initiatorEmail nothing is stored")
    void withoutInitiatorEmailNothingIsStored() {
        EmailInitiatorStore store = mock(EmailInitiatorStore.class);
        var listener = new IntakeListener(mock(org.springframework.kafka.core.KafkaTemplate.class), store);

        listener.trackEmailIntake("{\"workflowId\":\"wf-1\"}");

        verify(store, never()).storeInitiator(anyString(), anyString());
    }

    @Test
    @DisplayName("An empty initiatorEmail is not stored")
    void blankInitiatorIsIgnored() {
        EmailInitiatorStore store = mock(EmailInitiatorStore.class);
        var listener = new IntakeListener(mock(org.springframework.kafka.core.KafkaTemplate.class), store);

        listener.trackEmailIntake("{\"workflowId\":\"wf-1\",\"initiatorEmail\":\"  \"}");

        verify(store, never()).storeInitiator(anyString(), anyString());
    }

    @Test
    @DisplayName("Broken JSON is swallowed — the listener must not die")
    void malformedJsonDoesNotPropagate() {
        EmailInitiatorStore store = mock(EmailInitiatorStore.class);
        var listener = new IntakeListener(mock(org.springframework.kafka.core.KafkaTemplate.class), store);

        listener.trackEmailIntake("{ kaputt ,,, }");

        verify(store, never()).storeInitiator(anyString(), anyString());
    }

    @Test
    @DisplayName("handleIntake also acknowledges on a broken payload")
    void handleIntakeAlwaysAcknowledges() {
        Acknowledgment ack = mock(Acknowledgment.class);
        var listener = new IntakeListener(
                mock(org.springframework.kafka.core.KafkaTemplate.class),
                mock(EmailInitiatorStore.class));

        listener.handleIntake("{ kaputt ,,, }", ack);

        // deliberately: the listener does no domain processing (that is done by
        // DagTopicListener) and therefore always acknowledges, to avoid
        // to create an endless loop. This test pins that decision down.
        verify(ack).acknowledge();
    }

    // ── ConfigUpdateConsumer ─────────────────────────────────────────

    @Test
    @DisplayName("UPDATE liefert die neueste Version ab Version >= angefordert")
    void configUpdateQueriesAtLeastRequestedVersion() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class, Mockito.RETURNS_DEEP_STUBS);
        Acknowledgment ack = mock(Acknowledgment.class);
        var consumer = new ConfigUpdateConsumer(jdbc);

        consumer.handleConfigUpdate("""
                {"agent_type":"coder","version":3,"change_type":"UPDATE"}
                """, ack);

        verify(jdbc).query(contains("version >= ?"), any(org.springframework.jdbc.core.RowCallbackHandler.class),
                eq("coder"), eq(3));
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("DELETE deactivates the agent")
    void configDeleteDeactivatesAgent() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        var consumer = new ConfigUpdateConsumer(jdbc);

        consumer.handleConfigUpdate("""
                {"agent_type":"coder","version":1,"change_type":"DELETE"}
                """, ack);

        verify(jdbc).update(contains("is_active = false"), eq("coder"));
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("An unknown change_type is acknowledged but changes nothing")
    void unknownChangeTypeIsAcknowledgedWithoutAction() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        var consumer = new ConfigUpdateConsumer(jdbc);

        consumer.handleConfigUpdate("""
                {"agent_type":"coder","version":1,"change_type":"RENAMED"}
                """, ack);

        verify(jdbc, never()).update(anyString(), any(Object[].class));
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("A payload without version is acknowledged but not processed")
    void payloadWithoutVersionIsSkipped() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        var consumer = new ConfigUpdateConsumer(jdbc);

        consumer.handleConfigUpdate("{\"agent_type\":\"coder\"}", ack);

        verify(jdbc, never()).query(anyString(), any(org.springframework.jdbc.core.RowCallbackHandler.class),
                any(Object[].class));
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("A broken payload is acknowledged so that no replay loop arises")
    void malformedPayloadIsAcknowledged() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        var consumer = new ConfigUpdateConsumer(jdbc);

        consumer.handleConfigUpdate("{ kaputt ,,, }", ack);

        // deliberate decision: a retry loop on an unparseable payload
        // would block the consumer. This test pins that decision down.
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("DB errors on deactivating do not lead to a replay loop")
    void databaseFailureIsAcknowledged() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        Mockito.doThrow(new RuntimeException("db down"))
                .when(jdbc).update(anyString(), any(Object[].class));
        Acknowledgment ack = mock(Acknowledgment.class);
        var consumer = new ConfigUpdateConsumer(jdbc);

        consumer.handleConfigUpdate("""
                {"agent_type":"coder","version":1,"change_type":"DELETE"}
                """, ack);

        verify(ack).acknowledge();
    }
}
