package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.model.HitlTicketStatus;
import de.augmentia.agentplatform.orchestrator.db.JdbcCheckpointStore;
import de.augmentia.agentplatform.orchestrator.db.JdbcHitlTicketStore;
import de.augmentia.agentplatform.orchestrator.db.JdbcWorkflowDefinitionStore;
import de.augmentia.agentplatform.orchestrator.db.JdbcWorkflowExecutionStore;
import de.augmentia.agentplatform.orchestrator.domain.CheckpointRow;
import de.augmentia.agentplatform.orchestrator.domain.HitlTicketRow;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowDefinitionRow;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowExecutionRow;
import de.augmentia.agentplatform.orchestrator.web.CheckpointResponse;
import de.augmentia.agentplatform.orchestrator.web.HitlTicketResponse;
import de.augmentia.agentplatform.orchestrator.web.WorkflowStatusResponse;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class WorkflowQueryService {

    private final JdbcWorkflowExecutionStore executions;
    private final JdbcWorkflowDefinitionStore definitions;
    private final JdbcCheckpointStore checkpoints;
    private final JdbcHitlTicketStore tickets;

    public WorkflowQueryService(JdbcWorkflowExecutionStore executions,
                                JdbcWorkflowDefinitionStore definitions,
                                JdbcCheckpointStore checkpoints,
                                JdbcHitlTicketStore tickets) {
        this.executions = executions;
        this.definitions = definitions;
        this.checkpoints = checkpoints;
        this.tickets = tickets;
    }

    public WorkflowStatusResponse status(UUID executionId) {
        WorkflowExecutionRow execution = executions.findById(executionId)
                .orElseThrow(() -> new WorkflowNotFoundException(executionId.toString()));
        WorkflowDefinitionRow definition = definitions.findById(execution.workflowDefinitionId()).orElse(null);
        List<CheckpointResponse> checkpointResponses = checkpoints.findByExecution(executionId).stream()
                .map(WorkflowQueryService::toResponse)
                .toList();
        return new WorkflowStatusResponse(
                execution.id().toString(),
                definition == null ? null : definition.name(),
                definition == null ? 0 : definition.version(),
                execution.status().name(),
                execution.startedAt(),
                execution.completedAt(),
                parseJson(execution.finalOutputJson()),
                checkpointResponses
        );
    }

    public List<CheckpointResponse> checkpoints(UUID executionId, String stepId, Integer iteration) {
        executions.findById(executionId)
                .orElseThrow(() -> new WorkflowNotFoundException(executionId.toString()));
        return checkpoints.findByExecution(executionId).stream()
                .filter(row -> stepId == null || stepId.isBlank() || row.stepId().equals(stepId))
                .filter(row -> iteration == null || row.iteration() == iteration)
                .map(WorkflowQueryService::toResponse)
                .toList();
    }

    public List<HitlTicketResponse> tickets(HitlTicketStatus status) {
        List<HitlTicketRow> rows = status == null ? tickets.findByStatus(HitlTicketStatus.OPEN) : tickets.findByStatus(status);
        return rows.stream().map(WorkflowQueryService::toResponse).toList();
    }

    public HitlTicketRow requireTicket(UUID ticketId) {
        return tickets.findById(ticketId)
                .orElseThrow(() -> new IllegalArgumentException("HITL ticket not found: " + ticketId));
    }

    private static CheckpointResponse toResponse(CheckpointRow row) {
        return new CheckpointResponse(
                row.stepId(),
                row.iteration(),
                row.nodeType() == null ? null : row.nodeType().name(),
                row.agentType(),
                row.status().name(),
                row.decisionBranch(),
                row.taskId() == null ? null : row.taskId().toString(),
                row.attempt(),
                row.retryCount(),
                row.timeoutMs(),
                row.errorMessage(),
                row.startedAt(),
                row.completedAt(),
                parseJson(row.outputPayloadJson())
        );
    }

    private static HitlTicketResponse toResponse(HitlTicketRow row) {
        return new HitlTicketResponse(
                row.id().toString(),
                row.workflowExecutionId().toString(),
                row.stepId(),
                row.iteration(),
                row.reason(),
                row.status().name(),
                row.escalationCount(),
                row.expiresAt(),
                row.createdAt(),
                row.resolvedAt(),
                row.resolvedBy(),
                row.resolutionNotes(),
                parseJson(row.contextJson())
        );
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseJson(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            Map<String, Object> parsed = EventJson.read(json, Map.class);
            return parsed == null ? Map.of() : parsed;
        } catch (RuntimeException ex) {
            return Map.of("raw", json);
        }
    }
}
