package de.augmentia.agentplatform.orchestrator.web;

import de.augmentia.agentplatform.common.event.ControlEvents;
import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.agentplatform.orchestrator.kafka.OrchestratorEventPublisher;
import de.augmentia.agentplatform.orchestrator.service.WorkflowQueryService;
import de.augmentia.agentplatform.orchestrator.service.WorkflowStartService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/workflows")
public class WorkflowController {

    private final WorkflowStartService startService;
    private final WorkflowQueryService queryService;
    private final OrchestratorEventPublisher publisher;

    public WorkflowController(WorkflowStartService startService,
                              WorkflowQueryService queryService,
                              OrchestratorEventPublisher publisher) {
        this.startService = startService;
        this.queryService = queryService;
        this.publisher = publisher;
    }

    @PostMapping("/start")
    public ResponseEntity<StartWorkflowResponse> start(@RequestBody StartWorkflowRequest request) {
        if (request.idempotencyKey() == null || request.idempotencyKey().isBlank()) {
            throw new IllegalArgumentException("idempotencyKey is required");
        }
        WorkflowStartService.StartResult result = startService.start(
                request.workflowName(),
                request.version(),
                request.inputParams(),
                request.idempotencyKey()
        );
        StartWorkflowResponse body = new StartWorkflowResponse(result.executionId().toString(), result.duplicate());
        return ResponseEntity.status(result.duplicate() ? 200 : 202).body(body);
    }

    @GetMapping("/{executionId}")
    public WorkflowStatusResponse status(@PathVariable UUID executionId) {
        return queryService.status(executionId);
    }

    @GetMapping("/{executionId}/checkpoints")
    public List<CheckpointResponse> checkpoints(@PathVariable UUID executionId,
                                                @RequestParam(required = false) String stepId,
                                                @RequestParam(required = false) Integer iteration) {
        return queryService.checkpoints(executionId, stepId, iteration);
    }

    @PostMapping("/{executionId}/cancel")
    public ResponseEntity<Void> cancel(@PathVariable UUID executionId) {
        queryService.status(executionId);
        ControlEvents.CancellationEvent event = new ControlEvents.CancellationEvent(
                executionId.toString(),
                "Cancelled via REST API",
                "api"
        );
        publisher.publish(TopicNames.CANCELLATION, executionId.toString(), EventJson.write(event));
        return ResponseEntity.accepted().build();
    }
}
