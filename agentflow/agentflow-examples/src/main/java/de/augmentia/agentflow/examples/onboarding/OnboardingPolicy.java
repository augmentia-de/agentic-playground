package de.augmentia.agentflow.examples.onboarding;

import java.util.List;

/** Deterministic onboarding checklist (kept out of the LLM). */
public final class OnboardingPolicy {

    private OnboardingPolicy() {
    }

    public static List<String> firstWeekChecklist() {
        return List.of(
                "Account + SSO access provisioned",
                "Local dev environment running",
                "Read the architecture decision records",
                "Pair with a buddy on a starter task",
                "Ship one small change end-to-end");
    }
}
