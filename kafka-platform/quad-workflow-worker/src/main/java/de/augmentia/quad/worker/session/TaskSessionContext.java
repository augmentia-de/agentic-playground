package de.augmentia.quad.worker.session;

import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.agentplatform.common.event.TaskEvent;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Creates the session context per task from the TaskEvent.
 */
public class TaskSessionContext {

    private final String workspaceBase;

    public TaskSessionContext(String workspaceBase) {
        this.workspaceBase = workspaceBase;
    }

    /**
     * Creates a completely new AgentSessionState for a task.
     * Includes: Session-ID, Security-Context, Workspace-Grants, Memory.
     */
    public AgentSessionState create(TaskEvent task) {
        // Session-ID konstruieren (unique pro iteration/attempt)
        String sessionId = buildSessionId(task);
        
        AgentSessionState state = AgentSessionState.create(sessionId);
        state.setTenantId(task.header().workflowId());

        // Security context mit workflow/task metadata
        WorkerSecurityContext securityCtx = new WorkerSecurityContext(
            "anonymous",  // can be extracted from the task header if present
            Set.of("workflow-worker"),
            sessionId,
            Map.of(
                "workflowId", task.header().workflowId(),
                "taskId", task.header().taskId(),
                "traceParent", task.header().traceParent() != null ? task.header().traceParent() : ""
            )
        );
        state.setSecurityContext(securityCtx);

        // Session-Workspace granten
        String workflowDir = resolveWorkflowDir(task.header().workflowId());
        Path path = Path.of(workspaceBase).resolve(workflowDir).normalize();
        state.grantDir(path, true);

        // Memory initialisieren (default limit ~50 messages)
        state.setMemory(new de.augmentia.quad.core.session.memory.SessionMemory(50));

        return state;
    }

    private String buildSessionId(TaskEvent task) {
        return String.join(":",
            task.header().workflowId(),
            task.stepId(),
            String.valueOf(task.iteration()),
            String.valueOf(task.header().attempt()),
            UUID.randomUUID().toString().substring(0, 8)
        );
    }

    private String resolveWorkflowDir(String workflowId) {
        return "workflows/" + workflowId;
    }
}