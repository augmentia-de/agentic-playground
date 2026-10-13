package de.augmentia.agentplatform.bridge;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Map;

/**
 * A single node from the Quarkus flat workflow format.
 */
public record QuarkusNode(
        String id,
        String type,
    String agentType,
    String name,
    Map<String, Object> properties,
        Map<String, Object> config,
    Integer timeoutMs,
    Integer timeoutSeconds,
    Boolean requiresHumanApproval
) {
    /**
     * Convenience constructor for payloads without a {@code config} object.
     *
     * <p>The canonical constructor carries {@code config}; the 8-argument form is what the
     * quad-ui format produces and what most call sites use.
     */
    public QuarkusNode(String id, String type, String agentType, String name,
                       Map<String, Object> properties, Integer timeoutMs,
                       Integer timeoutSeconds, Boolean requiresHumanApproval) {
        this(id, type, agentType, name, properties, Map.of(), timeoutMs, timeoutSeconds,
                requiresHumanApproval);
    }

    @JsonCreator
    public QuarkusNode {
        id = id == null ? "" : id;
        type = type == null ? "agent" : type;
        properties = properties == null ? Map.of() : Map.copyOf(properties);
        config = config == null ? Map.of() : Map.copyOf(config);
    }

    /** Convert this node to its Kafka-orchestrator Edge representation. */
    public static class Builder {
        private String id;
        private String type;
        private String agentType;
        private String name;
        private Map<String, Object> properties = Map.of();
        private Map<String, Object> config = Map.of();
        private Integer timeoutMs;
        private Integer timeoutSeconds;
        private Boolean requiresHumanApproval;

        public Builder id(String v) { this.id = v; return this; }
        public Builder type(String v) { this.type = v; return this; }
        public Builder agentType(String v) { this.agentType = v; return this; }
        public Builder name(String v) { this.name = v; return this; }
        public Builder properties(Map<String, Object> v) { this.properties = v; return this; }
        public Builder config(Map<String, Object> v) { this.config = v; return this; }
        public Builder timeoutMs(Integer v) { this.timeoutMs = v; return this; }
        public Builder timeoutSeconds(Integer v) { this.timeoutSeconds = v; return this; }
        public Builder requiresHumanApproval(Boolean v) { this.requiresHumanApproval = v; return this; }

        public QuarkusNode build() {
            return new QuarkusNode(id, type, agentType, name, properties, config, timeoutMs,
                    timeoutSeconds, requiresHumanApproval);
        }
    }
}
