package de.augmentia.quad.workflow.engine.node.executor;

import de.augmentia.quad.workflow.engine.internal.GraphUtils;
import de.augmentia.quad.workflow.engine.internal.RunStateWriter;
import de.augmentia.quad.workflow.engine.internal.WorkflowExecutionContext;
import de.augmentia.quad.workflow.engine.internal.WorkflowSupport;
import de.augmentia.quad.workflow.engine.node.NodeConfig;
import de.augmentia.quad.workflow.engine.node.NodeExecutor;
import de.augmentia.quad.workflow.engine.node.NodeRunner;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Asynchronous node. Executes the referenced child node non-blockingly on a virtual
 * thread and waits up to {@code timeout} ms for its completion (local or via an external
 * deferred-completion callback). On timeout a marker is set as the result.
 */
public class AsyncNodeExecutor implements NodeExecutor {
    public AsyncNodeExecutor(final WorkflowSupport support, final AgentNodeExecutor agentExecutor, final RunStateWriter runWriter) {
        this.support = support;
        this.agentExecutor = agentExecutor;
        this.runWriter = runWriter;
    }

    private static final Logger log = Logger.getLogger(AsyncNodeExecutor.class);

    private final WorkflowSupport support;
    private final AgentNodeExecutor agentExecutor;
    private final RunStateWriter runWriter;

    @Override
    public Set<String> getTypes() {
        return Set.of("async");
    }

    @Override
    public boolean execute(Map<String, Object> node, List<Map<String, Object>> allNodes,
                           List<Map<String, Object>> edges, WorkflowExecutionContext ctx, NodeRunner runner) {
        String nodeId = NodeConfig.id(node);
        if (ctx.run() != null) runWriter.updateNode(ctx.run(), nodeId, "running", "", "async started");

        long timeout = support.nodeTimeoutMs(node, support.defaultTimeoutMs());

        String childId = NodeConfig.str(node, "refNodeId", "");
        if (childId.isBlank()) childId = GraphUtils.firstSuccessor(nodeId, edges);
        final String childIdFinal = childId;
        Map<String, Object> child = childIdFinal != null ? GraphUtils.nodeById(allNodes, childIdFinal) : null;
        if (child == null) {
            ctx.putOutput(nodeId, "[async: no child node]");
            if (ctx.run() != null) runWriter.updateAndPersist(ctx.run(), nodeId, "completed", "", "[async: no child node]");
            return true;
        }

        support.executor().submit(() -> {
            try {
                WorkflowExecutionContext childCtx = new WorkflowExecutionContext(
                    ctx.runId(), ctx.run(), ctx.initialData(), ctx.initialData(), support.newRunState(), false);
                // Single dispatch: runChildNode executes agent/messaging children itself and returns
                // their output; for every other child type it returns null and the child is run exactly
                // once via the NodeRunner.
                String out = agentExecutor.runChildNode(child, allNodes, edges, childCtx.outputs(), childCtx, childIdFinal);
                if (out == null) {
                    runner.runNode(child, allNodes, edges, childCtx, Set.of());
                    out = childCtx.output(childIdFinal);
                }
                synchronized (ctx.outputs()) {
                    ctx.putOutput(nodeId, out);
                }
                try {
                    support.completeDeferred(ctx.runId(), nodeId, out);
                } catch (IllegalStateException runEnded) {
                    // The run already finished (this node timed out and the engine moved on).
                    // Record locally; do not corrupt run state or report a node failure.
                    log.debugf("Async node %s finished after run %s ended: %s", nodeId, ctx.runId(), runEnded.getMessage());
                }
            } catch (Exception e) {
                if (ctx.run() != null) {
                    runWriter.updateAndPersist(ctx.run(), nodeId, "failed", "", e.getMessage());
                }
            } finally {
                synchronized (support.monitor) { support.monitor.notifyAll(); }
            }
        });

        // Block on the shared monitor until the deferred completion arrives (or the bound expires)
        // instead of polling with Thread.sleep — and never busy-wait forever when timeout==0 (B28).
        long waitBoundMs = timeout > 0 ? timeout : support.defaultTimeoutMs();
        boolean resolved;
        synchronized (support.monitor) {
            long deadline = System.currentTimeMillis() + waitBoundMs;
            while (!ctx.hasOutput(nodeId)) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) break;
                try {
                    support.monitor.wait(remaining);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            resolved = ctx.hasOutput(nodeId);
        }
        if (!resolved) {
            String marker = "[async: no result within " + waitBoundMs + "ms]";
            synchronized (ctx.outputs()) { ctx.putOutput(nodeId, marker); }
            if (ctx.run() != null) runWriter.updateNode(ctx.run(), nodeId, "completed", "", marker);
        } else if (ctx.run() != null) {
            runWriter.updateNode(ctx.run(), nodeId, "completed", "", ctx.output(nodeId));
        }
        if (ctx.run() != null) runWriter.persist(ctx.run());
        synchronized (support.monitor) { support.monitor.notifyAll(); }
        return true;
    }
}