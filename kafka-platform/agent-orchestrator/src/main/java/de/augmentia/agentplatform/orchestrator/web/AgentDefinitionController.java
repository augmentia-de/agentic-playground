package de.augmentia.agentplatform.orchestrator.web;

import de.augmentia.agentplatform.orchestrator.db.JdbcAgentDefinitionStore;
import de.augmentia.agentplatform.orchestrator.domain.AgentDefinitionRow;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** REST API for agent definitions (Kafka backend) — used by kafkaApi.ts. */
@RestController
@RequestMapping("/api/v1/agents")
public class AgentDefinitionController {

    private final JdbcAgentDefinitionStore store;

    public AgentDefinitionController(JdbcAgentDefinitionStore store) {
        this.store = store;
    }

    @GetMapping
    public List<AgentConfigResponse> list() {
        return store.list().stream().map(AgentDefinitionController::toResponse).toList();
    }

    @PostMapping
    public ResponseEntity<AgentConfigResponse> upsert(@RequestBody AgentUpsertRequest request) {
        validate(request);
        store.upsert(
                request.agentType(),
                request.description(),
                request.systemPrompt() == null ? "You are " + request.agentType() + ". Complete the task as specified." : request.systemPrompt(),
                request.modelName() == null ? "gpt-4o-mini" : request.modelName(),
                request.temperature() == null ? 0.20 : request.temperature(),
                request.maxTokens() == null ? 1024 : request.maxTokens(),
                request.timeoutSeconds() == null ? 60 : request.timeoutSeconds(),
                request.maxRetryAttempts() == null ? 3 : request.maxRetryAttempts(),
                request.version() == null ? 1 : request.version());
        return store.findByType(request.agentType())
                .map(AgentDefinitionController::toResponse)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.internalServerError().build());
    }

    private static void validate(AgentUpsertRequest r) {
        if (r == null || r.agentType() == null || r.agentType().isBlank()) {
            throw new IllegalArgumentException("agentType is required");
        }
        if (r.agentType().length() > 64) {
            throw new IllegalArgumentException("agentType must be <= 64 chars");
        }
    }

    private static AgentConfigResponse toResponse(AgentDefinitionRow r) {
        return new AgentConfigResponse(
                r.agentType(),
                r.description() == null ? r.agentType() : r.description(),
                "kafka",
                r.modelName(),
                r.temperature(),
                r.maxTokens(),
                r.timeoutSeconds(),
                r.maxRetryAttempts(),
                r.version(),
                r.maxReActIterations()
        );
    }

    public record AgentConfigResponse(
            String id,
            String name,
            String category,
            String modelKey,
            double temperature,
            int maxTokens,
            int timeoutSeconds,
            int maxRetryAttempts,
            int version,
            int maxReActIterations
    ) {}

    public record AgentUpsertRequest(
            String agentType,
            String description,
            String systemPrompt,
            String modelName,
            Double temperature,
            Integer maxTokens,
            Integer timeoutSeconds,
            Integer maxRetryAttempts,
            Integer version
    ) {}
}