package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.event.ControlEvents;
import de.augmentia.agentplatform.orchestrator.config.OrchestratorProperties;
import de.augmentia.agentplatform.orchestrator.db.JdbcCheckpointStore;
import de.augmentia.agentplatform.orchestrator.db.JdbcWorkflowExecutionStore;
import de.augmentia.agentplatform.orchestrator.domain.CheckpointRow;
import de.augmentia.agentplatform.orchestrator.domain.HitlTicketRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

@Service
public class SweepReaperService {

    private static final Logger log = LoggerFactory.getLogger(SweepReaperService.class);

    private final JdbcCheckpointStore checkpointStore;
    private final JdbcWorkflowExecutionStore executionStore;
    private final HitlTicketService hitlTicketService;
    private final WorkflowEngine engine;
    private final OrchestratorProperties properties;
    private final OrchestratorLeaseService lease;
    private final Clock clock;

    public SweepReaperService(JdbcCheckpointStore checkpointStore,
                              JdbcWorkflowExecutionStore executionStore,
                              HitlTicketService hitlTicketService,
                              WorkflowEngine engine,
                              OrchestratorProperties properties,
                              OrchestratorLeaseService lease,
                              Clock clock) {
        this.checkpointStore = checkpointStore;
        this.executionStore = executionStore;
        this.hitlTicketService = hitlTicketService;
        this.engine = engine;
        this.properties = properties;
        this.lease = lease;
        this.clock = clock;
    }

    /**
     * Sweep run over expired HITL tickets, orphaned checkpoints and
     * stuck executions.
     *
     * <p><b>Multi-instance:</b> the sweep is a periodic scheduled task and
     * runs on <em>every</em> orchestrator replica. Without a lease
     * replicas would reap the same checkpoints at the same time and escalate tickets.
     * {@link OrchestratorLeaseService} ensures that only one instance
     * works per cycle.
     *
     * <p>Without a lease the damage would be limited — the actions are
     * idempotent via {@code lock()} on the execution and {@code insertIfAbsent} on the
     * ticket. Even so, parallel runs double the DB load and
     * produce duplicate alerts.
     */
    public void runSweep(ControlEvents.SweepEvent event) {
        if (!lease.tryAcquire(OrchestratorLeaseService.LOCK_SWEEP)) {
            log.debug("Sweep lease held by another instance — skipping this cycle");
            return;
        }
        try {
            doSweep(event);
        } finally {
            lease.release(OrchestratorLeaseService.LOCK_SWEEP);
        }
    }

    private void doSweep(ControlEvents.SweepEvent event) {
        Instant now = event == null || event.generatedAt() == null ? clock.instant() : event.generatedAt();

        for (HitlTicketRow ticket : hitlTicketService.findExpired()) {
            safeRun("expire-ticket", ticket.id(), () -> engine.expireTicket(ticket.id()));
        }

        for (CheckpointRow stale : checkpointStore.findStaleInProgress(properties.orchestration().sweepStaleGraceMs(), now)) {
            safeRun("stale-checkpoint", stale.id(), () -> engine.handleStaleCheckpoint(stale.id()));
        }

        for (UUID executionId : executionStore.findRunningStalledByFailed()) {
            safeRun("stalled-execution", executionId, () -> engine.pauseStalledExecution(executionId, "STALLED_FAILED_BRANCH"));
        }

        Instant recoveryCutoff = now.minusMillis(properties.orchestration().sweepIntervalMs() * 2);
        for (UUID executionId : executionStore.findRunningUpdatedBefore(recoveryCutoff)) {
            safeRun("recover-execution", executionId, () -> engine.advance(executionId));
        }
    }

    private void safeRun(String scope, UUID id, Runnable action) {
        try {
            action.run();
        } catch (RuntimeException ex) {
            log.warn("Sweep action failed scope={} id={}", scope, id, ex);
        }
    }
}
