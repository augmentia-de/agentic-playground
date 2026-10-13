package de.augmentia.quad.worker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.quad.worker.agent.WorkerAgentFactory;
import de.augmentia.quad.worker.agent.WorkerTestFixtures;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Level 3 — item-1 DB-E2E: {@link JdbcAgentConfigSource} against a real
 * PostgreSQL instance (testcontainer) with the real orchestrator schema
 * {@code V1__init.sql} (agent_definitions).
 *
 * Verifiziert:
 * - SQL against the real agent_definitions table (column names exactly!)
 * - RowMapper 8-Felder-Mapping (tools_config JSONB → allowedTools Set)
 * - Defaults for fields without a DB column (maxReActIterations, structuredOutput)
 * - DB-first Vorrang vor YAML (gleiche Reihenfolge wie in QuadWorkerConfig)
 * - Factory uses the config found in the DB (SystemPrompt assertion)
 *
 * No Spring context, no real worker: AI-generated, but a real pipeline.
 */
@Testcontainers
class JdbcAgentConfigSourceE2ETest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
        .withDatabaseName("agent_platform")
        .withUsername("agent")
        .withPassword("agent");

    private static JdbcTemplate jdbc;

    @BeforeAll
    static void setUpSchema() {
        var ds = new org.springframework.jdbc.datasource.DriverManagerDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUsername(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        jdbc = new JdbcTemplate(ds);

        // Mediation: execute the schema directly (no Flyway in the worker module)
        String v1 = """
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
            CREATE TABLE agent_definitions (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                agent_type VARCHAR(64) NOT NULL UNIQUE,
                description TEXT,
                system_prompt TEXT NOT NULL,
                model_name VARCHAR(64) NOT NULL DEFAULT 'mock-deterministic',
                temperature NUMERIC(3, 2) NOT NULL DEFAULT 0.00,
                max_tokens INT NOT NULL DEFAULT 4096,
                tools_config JSONB NOT NULL DEFAULT '[]'::jsonb,
                timeout_seconds INT NOT NULL DEFAULT 60,
                max_retry_attempts INT NOT NULL DEFAULT 3,
                version INT NOT NULL DEFAULT 1,
                max_react_iterations INT NOT NULL DEFAULT 10,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """;
        jdbc.execute(v1);

        // An agent in the shared DB (as written by the orchestrator)
        jdbc.update("""
            INSERT INTO agent_definitions
                (agent_type, description, system_prompt, model_name, temperature, max_tokens, tools_config)
            VALUES (?, ?, ?, ?, ?, ?, ?::jsonb)
            """,
            "researcher",
            "DB-basierter Researcher",
            "DB-FIRST-SYSTEM-PROMPT-V1",
            "simple",
            0.42,
            2048,
            "[\"readFile\", \"webSearch\"]");
    }

    private JdbcAgentConfigSource source() {
        return new JdbcAgentConfigSource(jdbc, new ObjectMapper());
    }

    @Test
    void findsAgentFromSharedDb() {
        Optional<WorkerProperties.AgentConfig> config = source().find("researcher");

        assertThat(config).isPresent();
        WorkerProperties.AgentConfig c = config.get();
        assertThat(c.agentId()).isEqualTo("researcher");
        assertThat(c.modelKey()).isEqualTo("simple");
        assertThat(c.temperature()).isEqualTo(0.42);
        assertThat(c.maxOutputTokens()).isEqualTo(2048);
        assertThat(c.systemPrompt()).isEqualTo("DB-FIRST-SYSTEM-PROMPT-V1");
        assertThat(c.allowedTools()).containsExactlyInAnyOrder("readFile", "webSearch");
        // The row was inserted without max_react_iterations, so the
        // column default 10 applies. Previously that was hard-coded because the column did not exist.
        assertThat(c.maxReActIterations()).isEqualTo(10);
        // structuredOutputEnabled still has no column.
        assertThat(c.structuredOutputEnabled()).isFalse();
    }

    @Test
    void readsReActIterationsFromTheColumn() {
        jdbc.update("UPDATE agent_definitions SET max_react_iterations = 42 WHERE agent_type = ?",
                "researcher");

        Optional<WorkerProperties.AgentConfig> config = source().find("researcher");

        assertThat(config).isPresent();
        assertThat(config.get().maxReActIterations())
                .as("the limit comes from the DB, not from a fixed value")
                .isEqualTo(42);
    }

    @Test
    void nonPositiveIterationsFallBackToTheConfiguredDefault() {
        // The column has a CHECK, but a row from an earlier migration
        // can still contain 0. The worker must not start on that — quad
        // rejects 0 in Agent.setReActMaxIterations with an exception.
        jdbc.update("UPDATE agent_definitions SET max_react_iterations = 0 WHERE agent_type = ?",
                "researcher");

        var configured = new JdbcAgentConfigSource(jdbc, new ObjectMapper(),
                java.time.Duration.ofSeconds(30), 7);

        assertThat(configured.find("researcher").orElseThrow().maxReActIterations())
                .isEqualTo(7);
    }

    @Test
    void theDefaultItselfIsConfigurable() {
        jdbc.update("UPDATE agent_definitions SET max_react_iterations = 0 WHERE agent_type = ?",
                "researcher");

        var configured = new JdbcAgentConfigSource(jdbc, new ObjectMapper(),
                java.time.Duration.ofSeconds(30), 25);

        assertThat(configured.find("researcher").orElseThrow().maxReActIterations())
                .isEqualTo(25);
    }

    @Test
    void emptyOptionalForUnknownAgentType() {
        assertThat(source().find("coder-unknown")).isEmpty();
    }

    @Test
    void emptyToolsConfigParsesToEmptySet() {
        jdbc.update("""
            INSERT INTO agent_definitions (agent_type, system_prompt, model_name, tools_config)
            VALUES (?, ?, ?, ?::jsonb)
            """, "no-tools", "Plain agent", "simple", "[]");

        Optional<WorkerProperties.AgentConfig> config = source().find("no-tools");
        assertThat(config).isPresent();
        assertThat(config.get().allowedTools()).isEmpty();
    }

    @Test
    void dbSourceWinsOverYamlInFactory() throws Exception {
        // Item 1: Geordnete Quellen via WorkerAgentFactory — DB zuerst.
        JdbcAgentConfigSource jdbcSource = source();
        WorkerProperties yamlProps = WorkerTestFixtures.baseProperties(java.util.Map.of("researcher",
            new WorkerProperties.AgentConfig(
                "researcher", "simple", 0.9, 3, null, Set.of(), false, "YAML-FALLBACK-PROMPT",
                null, null, false)));

        var yamlSource = new PropertiesAgentConfigSource(yamlProps);

        // The real worker catalog knows the builtin tools of the DB config
        // (readFile, webSearch) — hier als Registrierung im ToolCatalog.
        var mapper = new ObjectMapper();
        var argsMapper = new de.augmentia.quad.core.tool.ToolArgsMapper(mapper);
        var catalog = new de.augmentia.quad.worker.tool.ToolCatalog(argsMapper,
            Set.of(new ReadFileTool(), new WebSearchTool()));
        var toolFactory = new de.augmentia.quad.worker.tool.AgentToolRegistryFactory(catalog, argsMapper);

        WorkerAgentFactory factory = new WorkerAgentFactory(
            WorkerTestFixtures.registry(),
            toolFactory,
            null,
            new de.augmentia.quad.core.session.WorkspaceResolver(),
            new de.augmentia.quad.core.agent.runtime.IdempotencyStore(),
            new de.augmentia.quad.core.agent.runtime.OTelAgentTracer(),
            catalog,
            yamlProps,
            java.util.List.of(jdbcSource, yamlSource));

        var agent = factory.create("researcher");
        assertThat(agent.getSystemPrompt()).isEqualTo("DB-FIRST-SYSTEM-PROMPT-V1");
    }

    // ── Test tools (names like the worker builtins, so allowedTools match) ──

    public static class ReadFileTool {
        @de.augmentia.quad.core.annotation.Tool("Read a file from workspace")
        public String readFile(String path) {
            return "content of " + path;
        }
    }

    public static class WebSearchTool {
        @de.augmentia.quad.core.annotation.Tool("Search the web")
        public String webSearch(String query) {
            return "results for " + query;
        }
    }
}