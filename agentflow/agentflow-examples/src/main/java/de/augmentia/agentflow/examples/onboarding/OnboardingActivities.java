package de.augmentia.agentflow.examples.onboarding;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** Steps of the onboarding assistant case (memory + capability search + MCP). */
@ActivityInterface
public interface OnboardingActivities {

    /** Recalls project memory as an injection block. */
    @ActivityMethod
    String recallProject(String projectScope);

    /** Remembers an onboarding fact. */
    @ActivityMethod
    String rememberProject(String projectScope, String fact);

    /** Produces a personalised onboarding plan using the recalled memory. */
    @ActivityMethod
    String planOnboarding(String newHire, String memoryBlock);

    /** Lists tools of an MCP server (SSE URL); empty when not configured. */
    @ActivityMethod(name = "OnboardingListMcpTools")
    String listMcpTools(String serverUrl);
}
