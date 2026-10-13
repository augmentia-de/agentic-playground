package de.augmentia.agentplatform.bridge;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * An edge from the Quarkus flat workflow format: connects a source node to a target.
 */
public record QuarkusEdge(
        String source,
        String target,
        String condition,
        Map<String, Object> properties
) {
    /** Internal Kafka edge representation used during conversion. */
    public record Edge(String source, String target, String condition, Map<String, Object> properties) {}

    @JsonCreator
    public QuarkusEdge {
        source = source == null ? "" : source;
        target = target == null ? "" : target;
        properties = properties == null ? java.util.Map.of() : java.util.Map.copyOf(properties);
    }

    public Edge toKafkaEdge() {
        return new Edge(source, target, condition, properties);
    }
}
