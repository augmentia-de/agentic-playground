package de.augmentia.agentflow.examples;

import java.util.List;

/** The five sample business cases shipped with the examples module. */
public final class BusinessCases {

    private BusinessCases() {
    }

    /** Short description of a case for discovery (UI / docs). */
    public record Case(String id, String title, String endpoint, String capabilities) {
    }

    public static List<Case> all() {
        return List.of(
                new Case("support-triage", "Customer support triage",
                        "POST /examples/support-triage",
                        "structured output, memory, capability/RAG, HITL"),
                new Case("contract-review", "Contract review with approval",
                        "POST /examples/contract-review",
                        "PII anonymization, guardrail, structured output, HITL"),
                new Case("invoice-processing", "Invoice processing (durable saga)",
                        "POST /examples/invoice",
                        "saga/compensation, structured output"),
                new Case("market-research", "Market research",
                        "POST /examples/market-research",
                        "sub-agents, web tools"),
                new Case("onboarding", "Onboarding assistant",
                        "POST /examples/onboarding",
                        "long-term memory, capability search, MCP")
        );
    }
}
