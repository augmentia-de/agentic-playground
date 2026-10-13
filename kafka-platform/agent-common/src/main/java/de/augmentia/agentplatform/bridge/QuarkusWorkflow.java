package de.augmentia.agentplatform.bridge;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.List;

/**
 * Flat workflow definition from Quarkus/quad-ui: {@code {name, nodes[], edges[]}}.
 */
public record QuarkusWorkflow(
        String name,
    List<QuarkusNode> nodes,
    List<QuarkusEdge> edges
) {
    @JsonCreator
    public QuarkusWorkflow {
        nodes = nodes == null ? List.of() : nodes;
        edges = edges == null ? List.of() : edges;
    }
}
