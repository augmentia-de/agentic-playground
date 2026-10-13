package de.augmentia.agentflow.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * AgentFlow: durable, graph-based agent workflows.
 *
 * <p>Graph reasoning via LangGraph4j (Spring AI for the model calls), durability and
 * orchestration via Temporal. Separately deployable from quad-quarkus/kafka-platform.</p>
 */
@SpringBootApplication(scanBasePackages = "de.augmentia.agentflow")
public class AgentFlowApplication {

    public static void main(String[] args) {
        SpringApplication.run(AgentFlowApplication.class, args);
    }
}
