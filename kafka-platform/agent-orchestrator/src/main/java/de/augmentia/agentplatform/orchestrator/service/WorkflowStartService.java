package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.event.WorkflowStartEvent;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.agentplatform.orchestrator.db.JdbcWorkflowExecutionStore;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowDefinitionRow;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowExecutionRow;
import de.augmentia.agentplatform.orchestrator.kafka.OrchestratorEventPublisher;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
public class WorkflowStartService {

    private final WorkflowDefinitionService definitions;
    private final JdbcWorkflowExecutionStore executions;
    private final StartIdempotencyGuard idempotencyGuard;
    private final OrchestratorEventPublisher publisher;
    private final Clock clock;

    public WorkflowStartService(WorkflowDefinitionService definitions,
                                JdbcWorkflowExecutionStore executions,
                                StartIdempotencyGuard idempotencyGuard,
                                OrchestratorEventPublisher publisher,
                                Clock clock) {
        this.definitions = definitions;
        this.executions = executions;
        this.idempotencyGuard = idempotencyGuard;
        this.publisher = publisher;
        this.clock = clock;
    }

    @Transactional
    public StartResult start(String workflowName, Integer version, Map<String, Object> inputParams, String idempotencyKey) {
        if (workflowName == null || workflowName.isBlank()) {
            throw new IllegalArgumentException("workflowName is required");
        }
        Map<String, Object> input = inputParams == null ? Map.of() : inputParams;

        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            Optional<WorkflowExecutionRow> existing = executions.findByIdempotencyKey(idempotencyKey);
            if (existing.isPresent()) {
                return new StartResult(existing.get().id(), true);
            }
        }

        WorkflowDefinitionRow definition = definitions.requireForStart(workflowName, version);
        UUID executionId = UUID.randomUUID();

        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            Optional<String> reservedExecutionId = idempotencyGuard.reserve(idempotencyKey, executionId.toString());
            if (reservedExecutionId.isPresent()) {
                Optional<WorkflowExecutionRow> existing = findExecution(reservedExecutionId.get());
                if (existing.isPresent()) {
                    return new StartResult(existing.get().id(), true);
                }
            }
        }

        try {
            executions.create(executionId, definition.id(), idempotencyKey, EventJson.write(input));
        } catch (DuplicateKeyException ex) {
            if (idempotencyKey != null && !idempotencyKey.isBlank()) {
                WorkflowExecutionRow existing = executions.findByIdempotencyKey(idempotencyKey)
                        .orElseThrow(() -> ex);
                return new StartResult(existing.id(), true);
            }
            throw ex;
        }

        WorkflowStartEvent event = new WorkflowStartEvent(
                executionId.toString(),
                definition.id().toString(),
                definition.name(),
                definition.version(),
                input,
                idempotencyKey,
                clock.instant()
        );
        publisher.publishAfterCommit(TopicNames.INTAKE, executionId.toString(), EventJson.write(event));
        return new StartResult(executionId, false);
    }

    private Optional<WorkflowExecutionRow> findExecution(String executionId) {
        try {
            return executions.findById(UUID.fromString(executionId));
        } catch (IllegalArgumentException ex) {
            return Optional.empty();
        }
    }

    public record StartResult(UUID executionId, boolean duplicate) {}
}
