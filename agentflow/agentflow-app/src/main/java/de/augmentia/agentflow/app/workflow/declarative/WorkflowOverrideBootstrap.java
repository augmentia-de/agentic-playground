package de.augmentia.agentflow.app.workflow.declarative;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/** Applies persisted overrides on top of the classpath definitions at startup. */
@Component
public class WorkflowOverrideBootstrap implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(WorkflowOverrideBootstrap.class);

    private final WorkflowDefinitionRegistry registry;
    private final WorkflowDefinitionStore store;

    public WorkflowOverrideBootstrap(WorkflowDefinitionRegistry registry, WorkflowDefinitionStore store) {
        this.registry = registry;
        this.store = store;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!store.enabled()) {
            return;
        }
        for (JsonNode override : store.loadOverrides()) {
            try {
                registry.register(registry.validate(override));
                log.info("Applied override '{}'", override.path("mainWorkflow").path("id").asText());
            } catch (RuntimeException e) {
                log.warn("Skipping invalid override: {}", e.getMessage());
            }
        }
    }
}
