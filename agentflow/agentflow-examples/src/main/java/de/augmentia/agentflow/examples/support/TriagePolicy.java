package de.augmentia.agentflow.examples.support;

/** Deterministic triage rules kept out of the LLM so they are reproducible and testable. */
public final class TriagePolicy {

    private TriagePolicy() {
    }

    /** Escalate when the classifier marked the ticket as HIGH priority. */
    public static boolean requiresEscalation(String assessmentJson) {
        if (assessmentJson == null) {
            return false;
        }
        String lower = assessmentJson.toLowerCase();
        return lower.contains("\"priority\"") && lower.contains("high");
    }
}
