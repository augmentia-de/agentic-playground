package de.augmentia.agentplatform.bridge.redis;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * WP04 — the initiator Redis key was misspelled {@code email:initator:} (missing second {@code i}),
 * so nothing that looked it up could ever find it. These tests pin the corrected key shape.
 */
class EmailInitiatorStoreTest {

    private StringRedisTemplate redis;
    private ValueOperations<String, String> ops;
    private EmailInitiatorStore store;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redis = mock(StringRedisTemplate.class);
        ops = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(ops);
        store = new EmailInitiatorStore(redis);
    }

    @Test
    @DisplayName("Initiator key is spelled email:initiator:{workflowId}")
    void initiatorKeyIsSpelledCorrectly() {
        store.storeInitiator("wf-1", "alice@corp.example");

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(ops).setIfAbsent(key.capture(), eq("alice@corp.example"), any(Duration.class));

        assertThat(key.getValue()).isEqualTo("email:initiator:wf-1");
        assertThat(key.getValue()).doesNotContain("initator:");
    }

    @Test
    @DisplayName("Tenant-scoped initiator key keeps the tenant segment")
    void tenantScopedInitiatorKey() {
        store.storeInitiator("wf-1", "tenant-x", "alice@corp.example");

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(ops).setIfAbsent(key.capture(), eq("alice@corp.example"), any(Duration.class));

        assertThat(key.getValue()).isEqualTo("email:initiator:tenant-x:wf-1");
    }

    @Test
    @DisplayName("lookupInitiator reads the same key that storeInitiator writes")
    void lookupUsesTheSameKey() {
        when(ops.get("email:initiator:wf-1")).thenReturn("alice@corp.example");

        assertThat(store.lookupInitiator("wf-1")).isEqualTo("alice@corp.example");
        verify(ops).get("email:initiator:wf-1");
    }

    @Test
    @DisplayName("Intake dedup key keeps the email:intake: prefix")
    void intakeLookupKey() {
        store.registerIntake("m-1", "wf-1");

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(ops).set(key.capture(), eq("wf-1"), any(Duration.class));

        assertThat(key.getValue()).isEqualTo("email:intake:m-1");
    }

    @Test
    @DisplayName("isAlreadyProcessed checks the dedup key")
    void isAlreadyProcessedChecksDedupKey() {
        when(redis.hasKey("email:intake:m-1")).thenReturn(true);

        assertThat(store.isAlreadyProcessed("m-1")).isTrue();
        verify(redis).hasKey("email:intake:m-1");
    }
}
