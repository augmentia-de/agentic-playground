package de.augmentia.agentflow.examples.contract;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** Steps of the contract-review case. */
@ActivityInterface
public interface ContractActivities {

    /** Reviews the contract text (PII-masked, guardrailed) and returns structured JSON. */
    @ActivityMethod
    String analyzeContract(String contractText);

    /** Records the human decision (would persist to an audit store in production). */
    @ActivityMethod
    String recordDecision(String summary);
}
