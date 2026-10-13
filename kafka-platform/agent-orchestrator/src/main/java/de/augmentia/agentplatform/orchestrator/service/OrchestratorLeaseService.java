package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.orchestrator.config.OrchestratorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Distributed lease service using a JDBC-backed lock table (ShedLock pattern).
 * Ensures that only one orchestrator instance executes critical operations
 * such as advanceLocked() at any given time — preventing race conditions
 * when multiple replicas process the same Kafka intake topic.
 *
 * <h3>Licensed Workflow</h3>
 * <pre>
 * 1. Acquire: INSERT ... ON CONFLICT DO UPDATE ... WHERE lock_until &lt;= now
 *    The competing write only wins if the previous lock has expired.
 *    If another instance holds the lock the WHERE condition does not match, no row
 *    is returned, and tryAcquire yields false (fail-closed).
 * 2. Execute: the winning node runs its scheduled task
 * 3. Release: DELETE WHERE name = ? AND locked_by = ? — releases only the own lock
 *    Alternatively the lock expires automatically after the TTL
 * </pre>
 *
 * <p>Used by {@link SweepReaperService} for the periodic sweep run.
 * Kafka consumers with their own consumer group need no lease — there
 * Kafka already serialises.
 */
@Service
public class OrchestratorLeaseService {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorLeaseService.class);

    /** Default lock name for the main orchestration loop */
    public static final String LOCK_ADVANCE = "orchestration-advance";
    /** Lock name for sweep reaper task */
    public static final String LOCK_SWEEP = "sweep-reaper";
    /** Lock name for config reload */
    public static final String LOCK_CONFIG_RELOAD = "config-reload";

    private final JdbcTemplate jdbcTemplate;
    private final Clock clock;
    private final String nodeId;
    private final long lockExpirationMs;

    public OrchestratorLeaseService(JdbcTemplate jdbcTemplate,
                                    @Value("${app.lease.instance-id:${HOSTNAME}}") String nodeId,
                                    OrchestratorProperties properties) {
        this.jdbcTemplate = jdbcTemplate;
        this.clock = Clock.systemUTC();
        this.nodeId = nodeId.trim().isEmpty() ? "unknown" : nodeId;
        this.lockExpirationMs = Duration.ofMinutes(properties.lease().lockExpirationMinutes()).toMillis();
    }

    /**
     * Attempts to acquire the named lock. Fail-closed: every error
     * (DB unreachable, conflict, no row result) yields {@code false}.
     *
     * <p><b>Time parameter:</b> {@code Instant} must be converted to
     * {@link Timestamp} before binding. In the generic varargs path of
     * {@code JdbcTemplate} Spring cannot determine the SQL type for {@code Instant}
     * ("cannot determine data type of java.time.Instant") and throws —
     * the {@code catch} would swallow that and yield {@code false}. The
     * lease would thereby be permanently without effect, unnoticed. The other
     * stores use the same conversion (see {@code timestamp(...)} in
     * {@code JdbcHitlTicketStore}).
     */
    public boolean tryAcquire(String lockName) {
        String sql = """
            INSERT INTO shedlock (name, lock_until, locked_at, locked_by)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (name) DO UPDATE SET 
                lock_until = EXCLUDED.lock_until,
                locked_at = EXCLUDED.locked_at,
                locked_by = EXCLUDED.locked_by
            WHERE shedlock.lock_until <= CURRENT_TIMESTAMP
            RETURNING shedlock.lock_until > CURRENT_TIMESTAMP
            """;

        Instant now = Instant.now(clock);
        Instant until = now.plusMillis(lockExpirationMs);

        try {
            Boolean result = jdbcTemplate.queryForObject(sql, Boolean.class,
                    lockName, Timestamp.from(until), Timestamp.from(now), nodeId);
            return result != null && result;
        } catch (Exception e) {
            log.warn("Failed to acquire lease '{}': {}", lockName, e.getMessage());
            return false;
        }
    }

    private static Timestamp timestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    /**
     * Release a lock by updating its lock_until to now (or simply delete it).
     * Also cleans up expired locks older than 2 hours as a maintenance task.
     */
    public void release(String lockName) {
        String sql = "DELETE FROM shedlock WHERE name = ? AND locked_by = ?";
        jdbcTemplate.update(sql, lockName, nodeId);
        
        // Also clean up stale locks from dead nodes (> 2 hours old)
        cleanupStaleLocks();
    }

    /**
     * Check if we currently hold a specific lock.
     */
    public boolean holds(String lockName) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM shedlock WHERE name = ? AND locked_by = ? AND lock_until > CURRENT_TIMESTAMP",
                Integer.class, lockName, nodeId);
        return count != null && count > 0;
    }

    /**
     * Maintenance task: remove all locks older than 2 hours (dead nodes).
     * Runs every 5 minutes.
     */
    @Scheduled(fixedRate = 300_000) // 5 min
    public void cleanupStaleLocks() {
        int deleted = jdbcTemplate.update("""
            DELETE FROM shedlock 
            WHERE locked_at < CURRENT_TIMESTAMP - INTERVAL '2 hours'
            """);
        if (deleted > 0) {
            log.debug("Cleaned up {} stale locks", deleted);
        }
    }

    /**
     * List all active locks (for monitoring/debugging).
     */
    public String listActiveLocks() {
        return jdbcTemplate.query(
                "SELECT name, locked_by, lock_until FROM shedlock WHERE lock_until > CURRENT_TIMESTAMP ORDER BY lock_until",
                (rs, i) -> String.format("[%s] %s (until %s)",
                        rs.getString("name"),
                        rs.getString("locked_by"),
                        rs.getTimestamp("lock_until"))
        ).stream().toList().toString();
    }
}
