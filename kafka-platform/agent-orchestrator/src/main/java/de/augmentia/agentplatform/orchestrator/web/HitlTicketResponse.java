package de.augmentia.agentplatform.orchestrator.web;

import java.time.Instant;
import java.util.Map;

public record HitlTicketResponse(
        String ticketId,
        String workflowExecutionId,
        String stepId,
        int iteration,
        String reason,
        String status,
        int escalationCount,
        Instant expiresAt,
        Instant createdAt,
        Instant resolvedAt,
        String resolvedBy,
        String resolutionNotes,
        Map<String, Object> context
) {}
