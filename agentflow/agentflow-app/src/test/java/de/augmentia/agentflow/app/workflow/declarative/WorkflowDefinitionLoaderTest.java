package de.augmentia.agentflow.app.workflow.declarative;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every bundled declarative definition must pass the quad-workflow-core publish compiler
 * (v2 JSON schema) and the semantic validator.
 */
class WorkflowDefinitionLoaderTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final WorkflowDefinitionLoader loader = new WorkflowDefinitionLoader(mapper);

    @Test
    void allBundledDefinitionsAreLoadedAndValid() throws Exception {
        Resource[] resources = new PathMatchingResourcePatternResolver()
                .getResources("classpath*:workflows/*.json");
        assertThat(resources).hasSizeGreaterThanOrEqualTo(5);

        Set<String> ids = new HashSet<>();
        for (Resource resource : resources) {
            try (InputStream in = resource.getInputStream()) {
                JsonNode doc = loader.load(in);

                assertThat(doc.path("schemaVersion").asInt()).isEqualTo(2);
                assertThat(doc.path("mainWorkflow").path("status").asText()).isEqualTo("published");
                assertThat(doc.path("contentHash").asText()).startsWith("sha256:");

                boolean hasAgentNode = false;
                for (JsonNode node : doc.path("mainWorkflow").path("nodes")) {
                    if ("agent".equals(node.path("type").asText())) {
                        hasAgentNode = true;
                        assertThat(node.path("agent").path("contentHash").asText()).startsWith("sha256:");
                    }
                }
                assertThat(hasAgentNode).as("workflow %s has agent nodes", resource.getFilename()).isTrue();
                ids.add(doc.path("mainWorkflow").path("id").asText());
            }
        }

        assertThat(ids).contains(
                "support-triage", "contract-review", "invoice-processing", "market-research", "onboarding");
    }
}
