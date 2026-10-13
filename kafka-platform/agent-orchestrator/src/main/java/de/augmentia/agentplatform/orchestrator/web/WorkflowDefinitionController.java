package de.augmentia.agentplatform.orchestrator.web;

import de.augmentia.agentplatform.orchestrator.definition.WorkflowDefinitionUpload;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowDefinitionRow;
import de.augmentia.agentplatform.orchestrator.service.WorkflowDefinitionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/workflow-definitions")
public class WorkflowDefinitionController {

    private final WorkflowDefinitionService definitions;

    public WorkflowDefinitionController(WorkflowDefinitionService definitions) {
        this.definitions = definitions;
    }

    @GetMapping
    public List<WorkflowDefinitionSummary> list(@RequestParam(required = false) Boolean active) {
        return definitions.list(active).stream()
                .map(WorkflowDefinitionController::toSummary)
                .toList();
    }

    @PostMapping
    public ResponseEntity<CreateWorkflowDefinitionResponse> create(@RequestBody WorkflowDefinitionUpload upload) {
        UUID id = definitions.create(upload);
        WorkflowDefinitionRow row = definitions.requireById(id);
        return ResponseEntity.status(201).body(new CreateWorkflowDefinitionResponse(
                row.id().toString(),
                row.name(),
                row.version()
        ));
    }

    private static WorkflowDefinitionSummary toSummary(WorkflowDefinitionRow row) {
        return new WorkflowDefinitionSummary(
                row.id().toString(),
                row.name(),
                row.version(),
                row.active(),
                row.createdAt()
        );
    }
}
