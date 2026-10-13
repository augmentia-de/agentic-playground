package de.augmentia.agentplatform.orchestrator.service;

import java.util.UUID;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * FINDING — the resolution of a tie in the start path cannot work.
 *
 * <p>{@code WorkflowStartService.start} is annotated {@code @Transactional} and catches the
 * {@code DuplicateKeyException} from {@code executions.create(...)} in order to look up the
 * already-running run inside it and report it as {@code duplicate}.
 *
 * <p>Exactly that cannot succeed in PostgreSQL: after a constraint violation the entire
 * transaction is aborted (SQLSTATE {@code 25P02}, {@code current transaction is aborted}).
 * Every further statement in it is rejected until it has been rolled back. The catch block
 * therefore runs a query that fails with {@code 25P02}, and the caller gets HTTP 500
 * instead of the existing run.
 *
 * <p><b>Why this test sits here and not in the integration suite.</b> The failure only
 * shows when two starts race for the same key. A test that waits for that is a race and
 * would be green or red depending on load — not a test but a random number generator. This
 * test replaces the randomness with the database: it runs exactly the statement sequence
 * the catch block runs and shows that the second statement fails. That way the mechanism is
 * proven independently of timing.
 *
 * <p>Against real Postgres with the real migrations, for the same reason as in
 * {@link WorkflowEngineTestBase}: the semantics at stake here are transaction
 * semantics. A mock would not have them.
 */
@DisplayName("FINDING — DuplicateKeyException in the catch of a @Transactional method")
class TransactionalDuplicateRecoveryTest {

    @SuppressWarnings("resource")
    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("agent_platform")
                    .withUsername("test")
                    .withPassword("test");

    private static JdbcTemplate jdbc;
    private static TransactionTemplate tx;

    /** The table on which the behaviour is demonstrated — not the product's one. */
    private static final String TABELLE = "befund_idempotenz";

    @BeforeAll
    static void containerUndSchema() {
        POSTGRES.start();
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();

        var ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUsername(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());

        jdbc = new JdbcTemplate(ds);
        tx = new TransactionTemplate(
                new org.springframework.jdbc.datasource.DataSourceTransactionManager(ds));
        jdbc.execute("CREATE TABLE IF NOT EXISTS " + TABELLE
                + " (idempotency_key VARCHAR(128) PRIMARY KEY)");
    }

    @Test
    @DisplayName("after a constraint violation the same transaction rejects every further query")
    void nachConstraintVerstossIstDieTransaktionUnbrauchbar() {
        var key = UUID.randomUUID().toString();

        assertThatThrownBy(() -> tx.executeWithoutResult(status -> {
            jdbc.update("INSERT INTO " + TABELLE + " (idempotency_key) VALUES (?)", key);
            // The second attempt with the same key is the trigger: right here
            // executions.create(...) throws the DuplicateKeyException that start() catches.
            jdbc.update("INSERT INTO " + TABELLE + " (idempotency_key) VALUES (?)", key);
        }))
                .as("the violation must actually occur, otherwise the test checks nothing")
                .isInstanceOf(DuplicateKeyException.class);

        // And it is not just the transaction that is unusable, but the content as well
        // lost: the entire transaction is rolled back, not just the violating
        // INSERT. That is why this reads 0 and not 1 — the first run no longer exists
        // afterwards, and a read in the same block would have found it if it were still there.
        var nachRollback = tx.execute(status -> jdbc.queryForObject(
                "SELECT count(*) FROM " + TABELLE + " WHERE idempotency_key = ?",
                Long.class, key));

        assertThat(nachRollback)
                .as("after the rollback the first INSERT is gone as well")
                .isEqualTo(0L);

        // Only in a new transaction can work continue. That is the path
        // the resolution would have to take.
        var erneuterVersuch = tx.execute(status -> {
            jdbc.update("INSERT INTO " + TABELLE + " (idempotency_key) VALUES (?)", key);
            return jdbc.queryForObject("SELECT count(*) FROM " + TABELLE
                    + " WHERE idempotency_key = ?", Long.class, key);
        });

        assertThat(erneuterVersuch)
                .as("in a separate transaction writing and reading works again")
                .isEqualTo(1L);
    }

    @Test
    @DisplayName("die Nachfrage im selben catch-Block scheitert mit 25P02")
    void resolutionInCatchBlockFails() {
        var key = UUID.randomUUID().toString();

        Throwable error = org.assertj.core.api.Assertions.catchThrowable(() -> tx.executeWithoutResult(status -> {
            try {
                jdbc.update("INSERT INTO " + TABELLE + " (idempotency_key) VALUES (?)", key);
                jdbc.update("INSERT INTO " + TABELLE + " (idempotency_key) VALUES (?)", key);
            } catch (DuplicateKeyException ex) {
                // This is the structure from WorkflowStartService.start: the catch
                // answers the request by re-reading in the same transaction.
                jdbc.queryForObject("SELECT count(*) FROM " + TABELLE
                        + " WHERE idempotency_key = ?", Long.class, key);
            }
        }));

        var cause = causeText(error);

        assertThat(cause)
                .as("the query in the catch can no longer be answered — that is why a "
                        + "duplicate run becomes an HTTP 500 instead of a reference to the "
                        + "already running run")
                .contains("25P02");
    }

    /** PostgreSQL nennt SQLSTATE 25P02 als "current transaction is aborted". */
    private static String causeText(Throwable throwable) {
        var cause = throwable;
        while (cause != null) {
            if (cause.getMessage() != null && cause.getMessage().contains("25P02")) {
                return cause.getMessage();
            }
            cause = cause.getCause();
        }
        return throwable.getMessage() == null ? "" : throwable.getMessage();
    }
}