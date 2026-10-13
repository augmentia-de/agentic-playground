package de.augmentia.agentflow.app.workflow;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** Activities for the Human-in-the-Loop flow (graph run + resume after approval). */
@ActivityInterface
public interface HitlActivities {

    /** Runs the graph until it pauses before the reviewer. */
    @ActivityMethod
    HitlStart startGraph(String input, String threadId);

    /** Resumes the persisted graph with the human approval. */
    @ActivityMethod
    String resumeGraph(String threadId);

    record HitlStart(String threadId, String plan, String result) {
    }
}
