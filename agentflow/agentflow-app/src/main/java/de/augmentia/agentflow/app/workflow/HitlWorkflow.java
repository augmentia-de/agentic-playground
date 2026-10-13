package de.augmentia.agentflow.app.workflow;

import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/** Durable HITL workflow: pauses for approval, resumes the graph via signal. */
@WorkflowInterface
public interface HitlWorkflow {

    @WorkflowMethod
    String run(String input);

    /** Human decision; delivered by the Temporal signal channel. */
    @SignalMethod
    void decide(boolean approved);
}
