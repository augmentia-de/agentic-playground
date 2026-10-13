package de.augmentia.agentflow.examples.research;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** Steps of the market-research case (orchestrator + sub-agent). */
@ActivityInterface
public interface ResearchActivities {

    /** Orchestrator delegates to a specialised research sub-agent (hierarchy/handoff). */
    @ActivityMethod
    String research(String topic);

    /** Turns raw research into a concise executive brief. */
    @ActivityMethod
    String writeBrief(String research);
}
