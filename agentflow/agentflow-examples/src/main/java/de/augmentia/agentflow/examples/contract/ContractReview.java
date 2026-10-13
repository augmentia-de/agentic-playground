package de.augmentia.agentflow.examples.contract;

import java.util.List;

/** Structured output of the contract reviewer. */
public record ContractReview(String riskLevel, List<String> flaggedClauses, String recommendation) {
}
