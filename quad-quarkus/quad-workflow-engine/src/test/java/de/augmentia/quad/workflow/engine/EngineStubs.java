package de.augmentia.quad.workflow.engine;

import de.augmentia.quad.contract.ApiDtos;
import de.augmentia.quad.core.workflow.run.WorkflowNodeResult;
import de.augmentia.quad.core.workflow.run.WorkflowRunState;
import de.augmentia.quad.workflow.engine.internal.WorkflowExecutionContext;
import de.augmentia.quad.workflow.engine.node.NodeExecutor;
import de.augmentia.quad.workflow.engine.node.NodeRunner;
import de.augmentia.quad.workflow.engine.port.AgentInvocationPort;
import de.augmentia.quad.workflow.engine.port.RunEventPort;
import de.augmentia.quad.workflow.engine.port.StepCheckpointPort;
import de.augmentia.quad.workflow.engine.port.WorkflowDefinitionPort;
import de.augmentia.quad.workflow.engine.port.WorkflowRunPort;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Shared, dependency-free stubs for the workflow-engine tests. */
final class EngineStubs {

    private EngineStubs() {
    }

    static final class InMemoryRunPort implements WorkflowRunPort {
        final Map<String, WorkflowRunState> runs = new ConcurrentHashMap<>();
        boolean saved;

        @Override
        public WorkflowRunState create(String workflowId, List<String> nodes, String initialData) {
            var state = WorkflowRunState.start("run-" + runs.size(), workflowId);
            state.initialData = initialData == null ? "" : initialData;
            for (var n : nodes) {
                state.nodeResults.add(new WorkflowNodeResult(n, "", "", "pending", "", ""));
            }
            runs.put(state.runId, state);
            return state;
        }

        @Override
        public WorkflowRunState load(String runId) {
            return runs.get(runId);
        }

        @Override
        public void save(WorkflowRunState state) {
            saved = true;
            runs.put(state.runId, state);
        }
    }

    static final class InMemoryCheckpoints implements StepCheckpointPort {
        final Map<String, Checkpoint> saved = new ConcurrentHashMap<>();

        @Override
        public boolean save(String runId, int stepIndex, String runStatus,
                            String snapshotJson, String memorySummary) {
            saved.put(runId, new Checkpoint(runId, stepIndex, runStatus, snapshotJson, memorySummary));
            return true;
        }

        @Override
        public Optional<Checkpoint> loadLast(String runId) {
            return Optional.ofNullable(saved.get(runId));
        }
    }

    static final class RecordingEvents implements RunEventPort {
        final List<String> phases = new ArrayList<>();
        /** Captured run events as {eventType, payloadJson}. */
        final List<String[]> events = new ArrayList<>();
        long runCounter;

        @Override
        public void recordEvent(String runId, String eventType, String payloadJson) {
            events.add(new String[]{eventType, payloadJson});
        }

        @Override
        public void audit(String id, String timestamp, String agent, String event, String details) {
        }

        @Override
        public void upsertRunSession(String runId, String task, String result, String memorySummary,
                                     de.augmentia.quad.core.session.AgentSessionState sessionState) {
        }

        @Override
        public void recordLifecycle(String runId, String phase, java.time.Instant timestamp) {
            phases.add(phase);
        }

        @Override
        public void recordStep(String runId, String stepId, String kind, String status, long durMs) {
        }

        @Override
        public long incrementRunCounter() {
            return ++runCounter;
        }
    }

    /** Agent factory whose agents count how often they are invoked. */
    static final class CountingAgents implements AgentInvocationPort {
        final AtomicInteger invocations = new AtomicInteger();

        @Override
        public de.augmentia.quad.core.agent.Agent create(String agentId) {
            return new de.augmentia.quad.core.agent.Agent() {
                @Override
                public String run(String prompt, de.augmentia.quad.core.session.AgentSessionState state) {
                    invocations.incrementAndGet();
                    return "stub-antwort";
                }

                @Override
                protected de.augmentia.quad.core.session.AgentSessionState newSessionState() {
                    return new de.augmentia.quad.core.session.AgentSessionState();
                }
            };
        }

        @Override
        public long timeoutMsOrZero(String agentId) {
            return 0L;
        }

        @Override
        public void sendToOutbound(String channelName, String message) {
        }
    }

    /** Fixed definition; the engine only reads it via the port. */
    static final class StaticDefinitions implements WorkflowDefinitionPort {
        private final ApiDtos.WorkflowDef def;

        StaticDefinitions(ApiDtos.WorkflowDef def) {
            this.def = def;
        }

        @Override
        public ApiDtos.WorkflowDef find(String workflowId) {
            return def;
        }
    }

    /** Executor for one node type that always fails. */
    static final class ThrowingExecutor implements NodeExecutor {
        private final Set<String> types;

        ThrowingExecutor(String type) {
            this.types = Set.of(type);
        }

        @Override
        public Set<String> getTypes() {
            return types;
        }

        @Override
        public boolean execute(Map<String, Object> node, List<Map<String, Object>> allNodes,
                               List<Map<String, Object>> edges, WorkflowExecutionContext ctx,
                               NodeRunner runner) {
            throw new RuntimeException("boom");
        }
    }

    /** Executor that counts executions and writes an output. */
    static final class CountingExecutor implements NodeExecutor {
        final AtomicInteger executions = new AtomicInteger();
        private final Set<String> types;

        CountingExecutor(String type) {
            this.types = Set.of(type);
        }

        @Override
        public Set<String> getTypes() {
            return types;
        }

        @Override
        public boolean execute(Map<String, Object> node, List<Map<String, Object>> allNodes,
                               List<Map<String, Object>> edges, WorkflowExecutionContext ctx,
                               NodeRunner runner) {
            executions.incrementAndGet();
            ctx.putOutput(String.valueOf(node.get("id")), "counted");
            return true;
        }
    }
}
