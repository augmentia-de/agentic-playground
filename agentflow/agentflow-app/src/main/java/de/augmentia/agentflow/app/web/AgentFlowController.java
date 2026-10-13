package de.augmentia.agentflow.app.web;

import de.augmentia.agentflow.app.workflow.AgentFlowWorkflow;
import de.augmentia.agentflow.app.workflow.AuthorizedAgentWorkflow;
import de.augmentia.agentflow.app.workflow.HitlWorkflow;
import de.augmentia.agentflow.app.workflow.McpWorkflow;
import de.augmentia.agentflow.app.workflow.MemoryWorkflow;
import de.augmentia.agentflow.app.workflow.MetricsWorkflow;
import de.augmentia.agentflow.app.workflow.ProvenanceWorkflow;
import de.augmentia.agentflow.app.workflow.QuadAgentWorkflow;
import de.augmentia.agentflow.app.workflow.QuadSagaWorkflow;
import de.augmentia.agentflow.app.workflow.RichAgentWorkflow;
import de.augmentia.agentflow.app.workflow.SagaWorkflow;
import de.augmentia.agentflow.app.workflow.StructuredAgentWorkflow;
import de.augmentia.agentflow.app.workflow.SubAgentWorkflow;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

/**
 * REST entry point: starts a durable {@link AgentFlowWorkflow} on Temporal and exposes
 * basic capability info (proves the quad-core reuse).
 */
@RestController
public class AgentFlowController {

    static final String TASK_QUEUE = "agentflow";

    private final WorkflowClient workflowClient;

    public AgentFlowController(WorkflowClient workflowClient) {
        this.workflowClient = workflowClient;
    }

    @PostMapping("/agentflow/runs")
    public Map<String, Object> start(@RequestParam("input") String input) {
        String workflowId = "agentflow-" + UUID.randomUUID();
        AgentFlowWorkflow workflow = workflowClient.newWorkflowStub(
                AgentFlowWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(TASK_QUEUE)
                        .setWorkflowId(workflowId)
                        .build());
        WorkflowClient.start(workflow::run, input);
        return Map.of("workflowId", workflowId, "taskQueue", TASK_QUEUE, "started", true);
    }

    @PostMapping("/agentflow/runs/quad")
    public Map<String, Object> startQuad(@RequestParam("input") String input) {
        String workflowId = "agentflow-quad-" + UUID.randomUUID();
        QuadAgentWorkflow workflow = workflowClient.newWorkflowStub(
                QuadAgentWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(TASK_QUEUE)
                        .setWorkflowId(workflowId)
                        .build());
        WorkflowClient.start(workflow::run, input);
        return Map.of("workflowId", workflowId, "taskQueue", TASK_QUEUE, "started", true, "engine", "quad-core");
    }

    @PostMapping("/agentflow/runs/hitl")
    public Map<String, Object> startHitl(@RequestParam("input") String input) {
        String workflowId = "agentflow-hitl-" + UUID.randomUUID();
        HitlWorkflow workflow = workflowClient.newWorkflowStub(
                HitlWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(TASK_QUEUE)
                        .setWorkflowId(workflowId)
                        .build());
        WorkflowClient.start(workflow::run, input);
        return Map.of("workflowId", workflowId, "taskQueue", TASK_QUEUE, "started", true,
                "awaiting", "approval", "decide", "/agentflow/runs/hitl/" + workflowId + "/decide?approved=true");
    }

    /** Delivers the human decision to the paused workflow (Temporal signal). */
    @PostMapping("/agentflow/runs/hitl/{workflowId}/decide")
    public Map<String, Object> decide(@PathVariable String workflowId,
                                      @RequestParam("approved") boolean approved) {
        HitlWorkflow workflow = workflowClient.newWorkflowStub(HitlWorkflow.class, workflowId);
        workflow.decide(approved);
        return Map.of("workflowId", workflowId, "approved", approved);
    }

    /**
     * Starts a rich quad-core agent (tools, skills, guardrails, PII, audit).
     *
     * @param tools     comma-separated built-in tool names (empty = all)
     * @param skillsDir optional skills directory
     */
    @PostMapping("/agentflow/runs/quad-rich")
    public Map<String, Object> startQuadRich(@RequestParam("input") String input,
                                             @RequestParam(name = "tools", required = false) String tools,
                                             @RequestParam(name = "skillsDir", required = false) String skillsDir) {
        java.util.List<String> toolList = (tools == null || tools.isBlank())
                ? java.util.List.of()
                : java.util.Arrays.stream(tools.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
        String workflowId = "agentflow-rich-" + UUID.randomUUID();
        RichAgentWorkflow workflow = workflowClient.newWorkflowStub(
                RichAgentWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build());
        WorkflowClient.start(workflow::run, input, toolList, skillsDir);
        return Map.of("workflowId", workflowId, "taskQueue", TASK_QUEUE, "started", true,
                "engine", "quad-core", "tools", toolList);
    }

    /** Lists the quad-core capabilities reusable by AgentFlow (for UI config/introspection). */
    @GetMapping("/agentflow/quad/capabilities")
    public Map<String, Object> quadCapabilities() {
        return Map.of(
                "builtinTools", de.augmentia.agentflow.app.quad.QuadCapabilities.BUILTIN_TOOLS,
                "guardrails", de.augmentia.agentflow.app.quad.QuadGuardrails.GUARDRAIL_NAMES,
                "guardrailsOutput", de.augmentia.agentflow.app.quad.QuadGuardrails.GUARDRAIL_NAMES,
                "hooks", de.augmentia.agentflow.app.quad.QuadGuardrails.HOOK_NAMES,
                "piiMaskTypes", de.augmentia.agentflow.app.quad.QuadGuardrails.PII_MASK_TYPES,
                "audit", true,
                "skills", "JIT (load_skill / skill_search)");
    }

    /** Role catalogue for RBAC (for UI config). */
    @GetMapping("/agentflow/security/roles")
    public Map<String, Object> roles() {
        return Map.of("roles", de.augmentia.agentflow.app.quad.QuadSecurity.catalogue());
    }

    /** Starts a role-scoped (RBAC) quad agent run. */
    @PostMapping("/agentflow/runs/authorized")
    public Map<String, Object> startAuthorized(@RequestParam("input") String input,
                                               @RequestParam(name = "roles", required = false) String roles) {
        java.util.List<String> roleList = (roles == null || roles.isBlank())
                ? java.util.List.of()
                : java.util.Arrays.stream(roles.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
        String workflowId = "agentflow-auth-" + UUID.randomUUID();
        AuthorizedAgentWorkflow workflow = workflowClient.newWorkflowStub(
                AuthorizedAgentWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build());
        WorkflowClient.start(workflow::run, input, roleList);
        return Map.of("workflowId", workflowId, "taskQueue", TASK_QUEUE, "started", true,
                "roles", roleList, "engine", "quad-core RBAC");
    }

    /** Starts a durable saga (reserve → charge, with compensation on failure). */
    @PostMapping("/agentflow/runs/saga")
    public Map<String, Object> startSaga(@RequestParam("orderId") String orderId,
                                         @RequestParam(name = "amount", defaultValue = "50") double amount) {
        String workflowId = "agentflow-saga-" + UUID.randomUUID();
        SagaWorkflow workflow = workflowClient.newWorkflowStub(
                SagaWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build());
        WorkflowClient.start(workflow::run, orderId, amount);
        return Map.of("workflowId", workflowId, "started", true, "amount", amount,
                "hint", amount > 100 ? "expect compensation" : "expect completion");
    }

    /** Starts a durable wrapper around quad-core's session-level saga unwind. */
    @PostMapping("/agentflow/runs/quad-saga")
    public Map<String, Object> startQuadSaga(@RequestParam("input") String input) {
        String workflowId = "agentflow-qsaga-" + UUID.randomUUID();
        QuadSagaWorkflow workflow = workflowClient.newWorkflowStub(
                QuadSagaWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build());
        WorkflowClient.start(workflow::run, input);
        return Map.of("workflowId", workflowId, "started", true, "engine", "quad-core saga");
    }

    /** Typed structured output via a quad agent. */
    @PostMapping("/agentflow/runs/structured")
    public Map<String, Object> startStructured(@RequestParam("input") String input) {
        String workflowId = "agentflow-struct-" + UUID.randomUUID();
        StructuredAgentWorkflow workflow = workflowClient.newWorkflowStub(
                StructuredAgentWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build());
        WorkflowClient.start(workflow::run, input);
        return Map.of("workflowId", workflowId, "started", true, "schema", "ContentPlan(topic,sections,audience)");
    }

    /** Agent run with provenance tracking. */
    @PostMapping("/agentflow/runs/provenance")
    public Map<String, Object> startProvenance(@RequestParam("input") String input) {
        String workflowId = "agentflow-prov-" + UUID.randomUUID();
        ProvenanceWorkflow workflow = workflowClient.newWorkflowStub(
                ProvenanceWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build());
        WorkflowClient.start(workflow::run, input);
        return Map.of("workflowId", workflowId, "started", true, "engine", "quad-core provenance");
    }

    /** Long-term memory: remember (default) or recall a scope. */
    @PostMapping("/agentflow/runs/memory")
    public Map<String, Object> startMemory(@RequestParam(name = "op", defaultValue = "remember") String op,
                                           @RequestParam("scope") String scope,
                                           @RequestParam(name = "content", required = false) String content) {
        String workflowId = "agentflow-mem-" + UUID.randomUUID();
        MemoryWorkflow workflow = workflowClient.newWorkflowStub(
                MemoryWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build());
        WorkflowClient.start(workflow::run, op, scope, content == null ? "" : content);
        return Map.of("workflowId", workflowId, "started", true, "op", op, "scope", scope);
    }

    /** MCP interaction: list tools (op=list) or run a tool (op=run). */
    @PostMapping("/agentflow/runs/mcp")
    public Map<String, Object> startMcp(@RequestParam(name = "op", defaultValue = "list") String op,
                                        @RequestParam(name = "serverUrl", required = false) String serverUrl,
                                        @RequestParam(name = "tool", required = false) String tool,
                                        @RequestParam(name = "args", defaultValue = "{}") String args) {
        String workflowId = "agentflow-mcp-" + UUID.randomUUID();
        McpWorkflow workflow = workflowClient.newWorkflowStub(
                McpWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build());
        WorkflowClient.start(workflow::run, op, serverUrl, tool, args);
        return Map.of("workflowId", workflowId, "started", true, "op", op,
                "hint", (serverUrl == null || serverUrl.isBlank()) ? "set serverUrl (SSE)" : "ok");
    }

    /** Orchestrator delegating to a sub-agent (hierarchy / handoff). */
    @PostMapping("/agentflow/runs/subagents")
    public Map<String, Object> startSubAgents(@RequestParam("input") String input) {
        String workflowId = "agentflow-sub-" + UUID.randomUUID();
        SubAgentWorkflow workflow = workflowClient.newWorkflowStub(
                SubAgentWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build());
        WorkflowClient.start(workflow::run, input);
        return Map.of("workflowId", workflowId, "started", true, "engine", "quad-core sub-agent");
    }

    /** Runs an agent with MetricsHook (Micrometer) + TracingHook (OTel). */
    @PostMapping("/agentflow/runs/metrics")
    public Map<String, Object> startMetrics(@RequestParam("input") String input) {
        String workflowId = "agentflow-metrics-" + UUID.randomUUID();
        MetricsWorkflow workflow = workflowClient.newWorkflowStub(
                MetricsWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build());
        WorkflowClient.start(workflow::run, input);
        return Map.of("workflowId", workflowId, "started", true,
                "metricsEndpoint", "/actuator/prometheus");
    }

    @GetMapping("/agentflow/info")
    public Map<String, Object> info() {
        // Reuse of the existing quad-core building blocks (AgentConfig facade).
        var config = de.augmentia.quad.core.agent.AgentConfig.builder()
                .systemPrompt("agentflow")
                .maxReActIterations(5)
                .build();
        return Map.of(
                "engine", "agentflow",
                "graph", "langgraph4j",
                "durability", "temporal",
                "reusesQuadCore", true,
                "quadCoreMaxReActIterations", config.maxReActIterations());
    }
}
