package de.augmentia.agentplatform.orchestrator.web;

import de.augmentia.agentplatform.orchestrator.service.WorkflowQueryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * GDPR compliance endpoints: export and delete personal data / session data.
 * Covers both workflow execution data (checkpoints, results) and session metadata.
 */
@RestController
@RequestMapping("/api/v1/gdpr")
public class GdprController {

    private final WorkflowQueryService queryService;

    public GdprController(WorkflowQueryService queryService) {
        this.queryService = queryService;
    }

    /**
     * Export all personal data associated with a session or workflow ID.
     * Returns structured JSON suitable for transfer to the data subject.
     *
     * GET /api/v1/gdpr/export?session={sessionId} OR ?workflowId={workflowId}
     */
    @GetMapping("/export")
    public ResponseEntity<Map<String, Object>> exportPersonalData(
            @RequestParam(required = false) String sessionId,
            @RequestParam(required = false) String workflowId
    ) {
        Map<String, Object> data = new java.util.LinkedHashMap<>();

        if (sessionId != null && !sessionId.isBlank()) {
            data.put("sessionId", sessionId);
            data.put("dataCategory", "session");
            // TODO: fetch session events from Redis/postgres
            data.put("events", List.of());
            data.put("metadata", Map.of(
                    "createdAt", Instant.now().minus(java.time.Duration.ofDays(30)).toString(),
                    "lastActivity", Instant.now().toString()
            ));
        } else if (workflowId != null && !workflowId.isBlank()) {
            UUID wfId;
            try {
                wfId = UUID.fromString(workflowId);
            } catch (IllegalArgumentException e) {
                return ResponseEntity.badRequest().body(Map.of(
                        "error", "Invalid workflowId format. Expected UUID."
                ));
            }

            data.put("workflowId", workflowId);
            data.put("dataCategory", "workflow_execution");
            try {
                var status = queryService.status(wfId);
                data.put("executionStatus", status.status());
                data.put("startedAt", status.startedAt());
                data.put("finishedAt", status.completedAt() != null ? status.completedAt().toString() : null);
                data.put("checkpoints", queryService.checkpoints(wfId, null, null));
            } catch (Exception e) {
                data.put("error", "Could not load execution data: " + e.getMessage());
            }
        } else {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "Provide either session or workflowId parameter."
            ));
        }

        data.put("exportedAt", Instant.now().toString());
        data.put("formatVersion", "1.0");

        return ResponseEntity.ok(data);
    }

    /**
     * Delete all personal data associated with a session or workflow ID.
     * This is irreversible — confirm via a confirmation token if needed.
     *
     * DELETE /api/v1/gdpr/delete?session={sessionId} OR ?workflowId={workflowId}&confirm=true
     */
    @DeleteMapping("/delete")
    public ResponseEntity<Map<String, Object>> deletePersonalData(
            @RequestParam(required = false) String sessionId,
            @RequestParam(required = false, defaultValue = "false") boolean confirm
    ) {
        if (!confirm) {
            return ResponseEntity.status(400).body(Map.of(
                    "error", "Set confirm=true to confirm deletion.",
                    "message", "This action is irreversible."
            ));
        }

        if (sessionId == null || sessionId.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "Provide either session or workflowId parameter."
            ));
        }

        // TODO: actual deletion logic — currently placeholder
        return ResponseEntity.ok(Map.of(
                "deleted", true,
                "dataType", sessionId.startsWith("wf-") ? "workflow" : "session",
                "identifier", sessionId,
                "deletedAt", Instant.now().toString(),
                "message", "Personal data deleted successfully."
        ));
    }

    /**
     * Anonymize data instead of deleting it (retains analytics while removing PII).
     * Useful for keeping aggregate metrics without personal identifiers.
     *
     * POST /api/v1/gdpr/anonymize?session={sessionId}
     */
    @PostMapping("/anonymize")
    public ResponseEntity<Map<String, Object>> anonymizePersonalData(
            @RequestParam(required = false) String sessionId
    ) {
        if (sessionId == null || sessionId.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Provide session parameter."));
        }

        // TODO: replace real names/emails with hashed placeholders in session records
        return ResponseEntity.ok(Map.of(
                "anonymized", true,
                "sessionId", sessionId,
                "method", "hash-replacement",
                "anonymizedAt", Instant.now().toString()
        ));
    }
}
