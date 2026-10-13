package de.augmentia.agentplatform.orchestrator.web;

import de.augmentia.agentplatform.common.event.ControlEvents;
import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.agentplatform.common.model.HitlAction;
import de.augmentia.agentplatform.common.model.HitlTicketStatus;
import de.augmentia.agentplatform.orchestrator.domain.HitlTicketRow;
import de.augmentia.agentplatform.orchestrator.kafka.OrchestratorEventPublisher;
import de.augmentia.agentplatform.orchestrator.service.WorkflowQueryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Locale;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/hitl")
public class HitlController {

    private final WorkflowQueryService queryService;
    private final OrchestratorEventPublisher publisher;

    public HitlController(WorkflowQueryService queryService, OrchestratorEventPublisher publisher) {
        this.queryService = queryService;
        this.publisher = publisher;
    }

    @GetMapping("/tickets")
    public List<HitlTicketResponse> tickets(@RequestParam(required = false) String status) {
        if (status == null || status.isBlank()) {
            return queryService.tickets(null);
        }
        return queryService.tickets(HitlTicketStatus.valueOf(status.toUpperCase(Locale.ROOT)));
    }

    @PostMapping("/tickets/{ticketId}/{action}")
    public ResponseEntity<Void> act(@PathVariable UUID ticketId,
                                    @PathVariable String action,
                                    @RequestBody(required = false) HitlActionRequest request) {
        HitlTicketRow ticket = queryService.requireTicket(ticketId);
        HitlAction hitlAction = switch (action.toLowerCase(Locale.ROOT)) {
            case "retry", "approve" -> HitlAction.RETRY;
            case "skip" -> HitlAction.SKIP;
            case "abort" -> HitlAction.ABORT;
            default -> throw new IllegalArgumentException("Unsupported HITL action: " + action);
        };
        ControlEvents.HitlEvent event = new ControlEvents.HitlEvent(
                ticket.workflowExecutionId().toString(),
                ticket.id().toString(),
                ticket.stepId(),
                ticket.iteration(),
                hitlAction,
                request == null || request.userId() == null || request.userId().isBlank() ? "api-user" : request.userId()
        );
        publisher.publish(TopicNames.HITL, ticket.workflowExecutionId().toString(), EventJson.write(event));
        return ResponseEntity.accepted().build();
    }
}
