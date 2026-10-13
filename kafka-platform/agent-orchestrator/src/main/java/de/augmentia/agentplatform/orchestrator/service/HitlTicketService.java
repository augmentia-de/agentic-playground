package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.model.HitlTicketStatus;
import de.augmentia.agentplatform.orchestrator.config.OrchestratorProperties;
import de.augmentia.agentplatform.orchestrator.db.JdbcHitlTicketStore;
import de.augmentia.agentplatform.orchestrator.domain.HitlTicketRow;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
public class HitlTicketService {

    private final JdbcHitlTicketStore tickets;
    private final OrchestratorProperties properties;
    private final Clock clock;

    public HitlTicketService(JdbcHitlTicketStore tickets, OrchestratorProperties properties, Clock clock) {
        this.tickets = tickets;
        this.properties = properties;
        this.clock = clock;
    }

    public UUID createIfAbsent(UUID executionId, UUID checkpointId, String stepId, int iteration,
                               String reason, Map<String, Object> context) {
        Optional<HitlTicketRow> existing = tickets.findOpen(executionId, stepId, iteration, reason);
        if (existing.isPresent()) {
            return existing.get().id();
        }
        Map<String, Object> ticketContext = new LinkedHashMap<>(context == null ? Map.of() : context);
        ticketContext.putIfAbsent("executionId", executionId.toString());
        ticketContext.putIfAbsent("stepId", stepId);
        ticketContext.putIfAbsent("iteration", iteration);
        ticketContext.putIfAbsent("reason", reason);
        return tickets.insert(executionId, checkpointId, stepId, iteration, reason,
                EventJson.write(ticketContext), clock.instant().plus(Duration.ofHours(properties.hitl().ttlHours())));
    }

    public HitlTicketRow require(UUID ticketId) {
        return tickets.findById(ticketId)
                .orElseThrow(() -> new IllegalArgumentException("HITL ticket not found: " + ticketId));
    }

    public List<HitlTicketRow> list(HitlTicketStatus status) {
        return status == null ? tickets.findByStatus(HitlTicketStatus.OPEN) : tickets.findByStatus(status);
    }

    public List<HitlTicketRow> findExpired() {
        return tickets.findExpired(clock.instant());
    }
}
