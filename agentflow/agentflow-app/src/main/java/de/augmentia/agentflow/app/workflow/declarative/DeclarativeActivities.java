package de.augmentia.agentflow.app.workflow.declarative;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** Executes a declaratively defined workflow document. */
@ActivityInterface
public interface DeclarativeActivities {

    /** Resolves the flattened, executable plan of a loaded workflow definition. */
    @ActivityMethod
    DeclarativePlan plan(String workflowId);

    /** Runs a single agent step with the accumulated task/context. */
    @ActivityMethod
    String runStep(DeclarativeStep step, String task);
}
