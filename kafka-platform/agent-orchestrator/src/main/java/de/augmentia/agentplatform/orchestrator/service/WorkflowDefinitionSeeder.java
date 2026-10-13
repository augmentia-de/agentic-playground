package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.orchestrator.config.OrchestratorProperties;
import de.augmentia.agentplatform.orchestrator.definition.WorkflowDefinitionUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

@Component
public class WorkflowDefinitionSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(WorkflowDefinitionSeeder.class);

    private final WorkflowDefinitionService definitions;
    private final OrchestratorProperties properties;
    private final ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

    public WorkflowDefinitionSeeder(WorkflowDefinitionService definitions, OrchestratorProperties properties) {
        this.definitions = definitions;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (!properties.seeding().enabled()) {
            return;
        }
        Resource[] resources;
        try {
            resources = resolver.getResources(properties.seeding().locations());
        } catch (IOException ex) {
            log.warn("Workflow seeding skipped because locations {} could not be resolved", properties.seeding().locations(), ex);
            return;
        }

        Arrays.stream(resources)
                .sorted(Comparator.comparing(resource -> String.valueOf(resource.getFilename())))
                .forEach(this::seed);
    }

    private void seed(Resource resource) {
        try (InputStream input = resource.getInputStream()) {
            String json = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            WorkflowDefinitionUpload upload = EventJson.read(json, WorkflowDefinitionUpload.class);
            definitions.seed(upload).ifPresentOrElse(
                    id -> log.info("Seeded workflow definition {} v{} as {}", upload.name(), upload.version(), id),
                    () -> log.debug("Workflow definition {} v{} already exists", upload.name(), upload.version())
            );
        } catch (Exception ex) {
            throw new IllegalStateException("Failed seeding workflow definition from " + resource.getFilename(), ex);
        }
    }
}
