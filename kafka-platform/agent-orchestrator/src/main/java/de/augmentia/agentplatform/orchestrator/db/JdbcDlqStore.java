package de.augmentia.agentplatform.orchestrator.db;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public class JdbcDlqStore {

    private final JdbcTemplate jdbcTemplate;

    private final java.time.Clock clock;

    /**
     * @param clock source for all timestamps of this table. Deliberately not
     *              the {@code now()} of the database: {@code updated_at} is
     *              compared in {@code findRunningUpdatedBefore} against a cutoff
     *              derived from the same clock. Two clocks make this
     *              decision impossible.
     */
    public JdbcDlqStore(JdbcTemplate jdbcTemplate, java.time.Clock clock) {
        this.jdbcTemplate = jdbcTemplate;
        this.clock = clock;
    }

    public UUID insert(String topic, String originalKey, String payloadJson, String errorClass, String errorMessage) {
        UUID id = UUID.randomUUID();
        jdbcTemplate.update("""
                INSERT INTO dlq_messages (id, topic, original_key, payload, error_class, error_message, created_at)
                VALUES (?, ?, ?, ?::jsonb, ?, ?, ?)
                """, id, topic, originalKey, payloadJson, errorClass, errorMessage,
                timestamp(clock.instant()));
        return id;
    }

    private static java.sql.Timestamp timestamp(java.time.Instant instant) {
        return instant == null ? null : java.sql.Timestamp.from(instant);
    }
}
