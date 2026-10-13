package de.augmentia.agentflow.app.workflow.declarative;

import java.io.Serializable;
import java.util.List;

/** The flattened, executable plan of a declarative workflow (all node types, sub-workflows inlined). */
public record DeclarativePlan(List<NodeSpec> nodes) implements Serializable {
}
