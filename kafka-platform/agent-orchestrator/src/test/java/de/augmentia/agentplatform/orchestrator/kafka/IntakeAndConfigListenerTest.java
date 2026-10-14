package de.augmentia.agentplatform.orchestrator.kafka;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.Acknowledgment;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Config update listener.
 *
 * <p>The former {@code IntakeListener} (a no-op intake ack plus email-initiator tracking) was
 * removed in WP04/B1: intake is handled by {@code DagTopicListener}, and the initiator mapping is
 * now written directly by {@code EmailInputTransformer} keyed by the real execution id (see
 * {@code EmailInputTransformerTest}).
 */
@DisplayName("Config listener")
class IntakeAndConfigListenerTest {

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
