package de.augmentia.agentflow.app.workflow;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Durable saga: reserve → charge; if charging fails, compensate (release + refund).
 * The compensation is driven by deterministic workflow code, so it survives restarts.
 */
@WorkflowInterface
public interface SagaWorkflow {

    @WorkflowMethod
    String run(String orderId, double amount);
}
