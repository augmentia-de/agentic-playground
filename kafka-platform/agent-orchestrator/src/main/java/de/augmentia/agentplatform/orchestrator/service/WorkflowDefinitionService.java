package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.model.WorkflowDefinition;
import de.augmentia.agentplatform.common.routing.WorkflowDefinitionValidator;
import de.augmentia.agentplatform.orchestrator.db.JdbcWorkflowDefinitionStore;
import de.augmentia.agentplatform.orchestrator.definition.WorkflowDefinitionUpload;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowDefinitionRow;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class WorkflowDefinitionService {

    private final JdbcWorkflowDefinitionStore definitionStore;

    public WorkflowDefinitionService(JdbcWorkflowDefinitionStore definitionStore) {
        this.definitionStore = definitionStore;
    }

    @Transactional
    public UUID create(WorkflowDefinitionUpload upload) {
        if (upload == null || upload.name() == null || upload.name().isBlank()) {
            throw new WorkflowValidationException(List.of("name is required"));
        }
        int version = upload.version() == null ? 1 : upload.version();
        if (version < 1) {
            throw new WorkflowValidationException(List.of("version must be >= 1"));
        }
        if (upload.dagStructure() == null || upload.dagStructure().nodes() == null || upload.dagStructure().nodes().isEmpty()) {
            throw new WorkflowValidationException(List.of("dag_structure.nodes is required"));
        }

        WorkflowDefinition candidate;
        String dagStructureJson;
        try {
            candidate = upload.toDefinition(UUID.randomUUID().toString(), version, true);
            dagStructureJson = EventJson.write(upload.dagStructure());
        } catch (IllegalArgumentException ex) {
            throw new WorkflowValidationException(List.of("dag_structure parse error: " + ex.getMessage()));
        }

        List<String> violations = WorkflowDefinitionValidator.validate(candidate);
        if (!violations.isEmpty()) {
            throw new WorkflowValidationException(violations);
        }
        if (definitionStore.exists(upload.name(), version)) {
            throw new DuplicateWorkflowDefinitionException(upload.name(), version);
        }
        try {
            return definitionStore.insert(upload.name(), version, dagStructureJson, true);
        } catch (DuplicateKeyException ex) {
            throw new DuplicateWorkflowDefinitionException(upload.name(), version);
        }
    }

    @Transactional
    public Optional<UUID> seed(WorkflowDefinitionUpload upload) {
        if (upload == null || upload.name() == null || upload.name().isBlank()) {
            throw new WorkflowValidationException(List.of("name is required"));
        }
        int version = upload.version() == null ? 1 : upload.version();
        if (definitionStore.exists(upload.name(), version)) {
            return Optional.empty();
        }
        WorkflowDefinition candidate;
        String dagStructureJson;
        try {
            candidate = upload.toDefinition(UUID.randomUUID().toString(), version, true);
            dagStructureJson = EventJson.write(upload.dagStructure());
        } catch (IllegalArgumentException ex) {
            throw new WorkflowValidationException(List.of("dag_structure parse error: " + ex.getMessage()));
        }
        List<String> violations = WorkflowDefinitionValidator.validate(candidate);
        if (!violations.isEmpty()) {
            throw new WorkflowValidationException(violations);
        }
        try {
            return Optional.of(definitionStore.insert(upload.name(), version, dagStructureJson, true));
        } catch (DuplicateKeyException ex) {
            return Optional.empty();
        }
    }

    public WorkflowDefinitionRow requireForStart(String name, Integer version) {
        return definitionStore.findForStart(name, version)
                .orElseThrow(() -> new WorkflowDefinitionNotFoundException(
                        "Workflow definition not found: " + name + (version == null ? " (active)" : " v" + version)));
    }

    public WorkflowDefinitionRow requireById(UUID id) {
        return definitionStore.findById(id)
                .orElseThrow(() -> new WorkflowDefinitionNotFoundException("Workflow definition not found: " + id));
    }

    public WorkflowDefinition loadDefinition(UUID definitionId) {
        return toDefinition(requireById(definitionId));
    }

    public WorkflowDefinition toDefinition(WorkflowDefinitionRow row) {
        WorkflowDefinitionUpload.DagStructure structure =
                EventJson.read(row.dagStructureJson(), WorkflowDefinitionUpload.DagStructure.class);
        WorkflowDefinitionUpload upload = new WorkflowDefinitionUpload(row.name(), row.version(), structure);
        return upload.toDefinition(row.id().toString(), row.version(), row.active());
    }

    public List<WorkflowDefinitionRow> list(Boolean activeOnly) {
        return definitionStore.list(activeOnly);
    }

    public boolean exists(String name, int version) {
        return definitionStore.exists(name, version);
    }

    public UUID insertIfAbsent(String name, int version, String dagStructureJson, boolean activate) {
        if (definitionStore.exists(name, version)) {
            return definitionStore.findForStart(name, version)
                    .map(WorkflowDefinitionRow::id)
                    .orElseThrow(() -> new IllegalStateException("Definition disappeared during seeding: " + name));
        }
        try {
            return definitionStore.insert(name, version, dagStructureJson, activate);
        } catch (DuplicateKeyException ex) {
            return definitionStore.findForStart(name, version)
                    .map(WorkflowDefinitionRow::id)
                    .orElseThrow(() -> new IllegalStateException("Definition disappeared during seeding: " + name));
        }
    }
}
