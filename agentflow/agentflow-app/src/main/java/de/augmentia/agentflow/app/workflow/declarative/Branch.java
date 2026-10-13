package de.augmentia.agentflow.app.workflow.declarative;

import java.io.Serializable;

/** A decision/fork branch: condition (decision) and target node id. */
public record Branch(String branchId, String condition, String target) implements Serializable {
}
