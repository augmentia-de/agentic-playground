package de.augmentia.agentflow.examples.support;

import java.util.List;

/** Structured output of the ticket classifier. */
public record TicketAssessment(String category, String priority, String sentiment, String suggestedReply) {

    public static List<String> categories() {
        return List.of("billing", "technical", "account", "other");
    }
}
