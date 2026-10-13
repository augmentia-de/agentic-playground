package de.augmentia.agentflow.app.workflow;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** Capability search / RAG as durable activities. */
@ActivityInterface
public interface CapabilityActivities {

    /** Retrieves the top-k capabilities matching the query (optionally tenant-filtered) as JSON. */
    @ActivityMethod
    String searchCapabilities(String query, int topK, String tenant);

    /** Runs an agent that can discover tools/skills at runtime via {@code capability_search}. */
    @ActivityMethod
    String runWithCapabilitySearch(String input);
}
