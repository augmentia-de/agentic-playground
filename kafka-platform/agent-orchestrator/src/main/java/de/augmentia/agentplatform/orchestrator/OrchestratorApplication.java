package de.augmentia.agentplatform.orchestrator;

import de.augmentia.agentplatform.orchestrator.config.OrchestratorProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * <h3>Why {@code scanBasePackages} is set explicitly</h3>
 *
 * Without this specification Spring Boot only scans {@code de.augmentia.agentplatform.orchestrator.**}.
 * But seven components live in {@code de.augmentia.agentplatform.bridge.**}
 * ({@code EmailInitiatorStore}, {@code EmailInputTransformer},
 * {@code MailResponseHandler}, {@code KafkaProducer}, {@code ImapMailListener},
 * {@code SecretResolver}) and would never be registered. That was not a silent
 * misbehaviour but a <b>startup abort</b>: {@code IntakeListener} hard-requires
 * the {@code EmailInitiatorStore} in its constructor; without it the
 * application would not start.
 *
 * <p>That meant the complete email intake feature and the Vault resolution
 * were effectively absent, even though they are listed as implemented.
 * No test had noticed because none booted a Spring context.
 *
 * <p>{@code de.augmentia.agentplatform.common} (module agent-common) deliberately contains no
 * components, so the broader scanning is harmless.
 * Covered by {@code ComponentScanCoverageTest}.
 */
@SpringBootApplication(scanBasePackages = "de.augmentia.agentplatform")
@EnableScheduling
@EnableConfigurationProperties(OrchestratorProperties.class)
public class OrchestratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrchestratorApplication.class, args);
    }
}
