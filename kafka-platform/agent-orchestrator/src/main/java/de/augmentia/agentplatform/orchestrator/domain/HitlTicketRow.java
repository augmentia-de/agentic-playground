package de.augmentia.agentplatform.orchestrator.domain;

import de.augmentia.agentplatform.common.model.HitlTicketStatus;

import java.time.Instant;
import java.util.UUID;

public record HitlTicketRow(
        UUID id,
        UUID workflowExecutionId,
        UUID checkpointId,
        String stepId,
        int iteration,
        String reason,
        HitlTicketStatus status,
        String contextJson,
        int escalationCount,
        Instant expiresAt,
        Instant createdAt,
        Instant resolvedAt,
        String resolvedBy,
        String resolutionNotes
) {}
