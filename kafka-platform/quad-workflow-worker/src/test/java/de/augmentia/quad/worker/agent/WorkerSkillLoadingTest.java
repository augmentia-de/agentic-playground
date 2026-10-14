package de.augmentia.quad.worker.agent;

import de.augmentia.quad.core.tool.SkillLoadTool;
import de.augmentia.quad.core.tool.ToolArgsMapper;
import de.augmentia.quad.worker.config.ChatModelRegistry;
import de.augmentia.quad.worker.config.WorkerProperties;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class WorkerSkillLoadingTest {

    @Test
    void shouldRegisterLoadSkillToolWhenSkillsDirConfigured() throws Exception {
        Path skillsDir = Files.createTempDirectory("worker-skills");
        createSkill(skillsDir, "code-review", "Code review", "Review process");

        var factory = createFactory(skillsDir);
        var agent = factory.create("researcher");

        var registry = agent.getToolRegistry();
        assertNotNull(registry);
        var loadSkill = registry.get("load_skill");
        assertNotNull(loadSkill, "load_skill tool must be registered when skillsDir is configured");
        assertTrue(loadSkill instanceof SkillLoadTool);

        var result = loadSkill.execute("{\"skillName\":\"code-review\"}", null);
        assertTrue(result.text().contains("## Skill loaded: code-review"));
        assertTrue(result.text().contains("Review process"));
    }

    @Test
    void shouldNotRegisterSkillToolsWhenNoSkillsDir() throws Exception {
        var factory = createFactory(null);
        var agent = factory.create("researcher");

        var registry = agent.getToolRegistry();
        assertNull(registry.get("load_skill"),
            "load_skill must NOT be registered when no skillsDir is configured");
    }

    @Test
    void shouldLoadMultipleSkills() throws Exception {
        Path skillsDir = Files.createTempDirectory("worker-skills-multi");
        createSkill(skillsDir, "code-review", "Code review", "Review process");
        createSkill(skillsDir, "data-analysis", "Data analysis", "Analysis process");

        var factory = createFactory(skillsDir);
        var agent = factory.create("researcher");

        var loadSkill = agent.getToolRegistry().get("load_skill");
        assertNotNull(loadSkill);

        var r1 = loadSkill.execute("{\"skillName\":\"code-review\"}", null);
        assertTrue(r1.text().contains("Review process"));

        var r2 = loadSkill.execute("{\"skillName\":\"data-analysis\"}", null);
        assertTrue(r2.text().contains("Analysis process"));
    }

    @Test
    void shouldReturnErrorForUnknownSkillName() throws Exception {
        Path skillsDir = Files.createTempDirectory("worker-skills-unknown");
        createSkill(skillsDir, "known", "Known skill", "Body");

        var factory = createFactory(skillsDir);
        var agent = factory.create("researcher");

        var loadSkill = agent.getToolRegistry().get("load_skill");
        var result = loadSkill.execute("{\"skillName\":\"unknown\"}", null);
        assertTrue(result.text().contains("not found"));
        assertTrue(result.text().contains("known"));
    }

    private WorkerAgentFactory createFactory(Path skillsDir) {
        var props = new WorkerProperties(
            true, "quad-workflow-worker", "agent\\.tasks\\..*",
            "agent.execution.results", "agent.execution.dlq",
            10, 120000, "/tmp/quad-workspace",
            Map.of("researcher", new WorkerProperties.AgentConfig(
                "researcher", "simple", 0.7, 10, 4096,
                Set.of("readFile", "writeFile"),
                false, "You are a helpful assistant.",
                skillsDir != null ? skillsDir.toString() : null,
                null, false
            )),
            new WorkerProperties.KafkaConfig()
        );

        var argsMapper = new ToolArgsMapper(new com.fasterxml.jackson.databind.ObjectMapper());
        // the catalog needs the tools that allowedTools references — otherwise
        // scheitert die Registry-Aufloesung mit "Unknown tool: readFile".
        // Gleiche Tools, die QuadWorkerConfig als Builtins registriert.
        var catalog = new de.augmentia.quad.worker.tool.ToolCatalog(argsMapper, Set.of(
            new de.augmentia.quad.core.tool.builtin.ReadFileTool(),
            new de.augmentia.quad.core.tool.builtin.WriteTool()
        ));
        var registryFactory = new de.augmentia.quad.worker.tool.AgentToolRegistryFactory(catalog, argsMapper);

        return new WorkerAgentFactory(
        // the AgentConfig requires modelKey "simple" — an empty registry
            // liefert "Unknown model key: simple. Available: []". Der Mock reicht,
        // because these tests check the skill registration, not the LLM.
            WorkerTestFixtures.registryWith(new ChatModelRegistry.MockChatModel()),
            registryFactory,
            new de.augmentia.quad.core.session.CurrentSession(),
            new de.augmentia.quad.core.session.WorkspaceResolver(),
            new de.augmentia.quad.core.agent.runtime.IdempotencyStore(),
            new de.augmentia.quad.core.agent.runtime.OTelAgentTracer(),
            catalog,
            props
        );
    }

    private void createSkill(Path skillsDir, String name, String description, String body) throws Exception {
        Path skillDir = Files.createDirectories(skillsDir.resolve(name));
        Files.writeString(skillDir.resolve("SKILL.md"),
            "---\nname: " + name + "\ndescription: " + description + "\n---\n" + body + "\n");
    }
}