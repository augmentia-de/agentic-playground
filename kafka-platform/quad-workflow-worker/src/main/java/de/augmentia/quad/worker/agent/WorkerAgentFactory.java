package de.augmentia.quad.worker.agent;

import de.augmentia.quad.core.agent.Agent;
import de.augmentia.quad.core.agent.AgentBuilder;
import de.augmentia.quad.core.agent.AgentConfig;
import de.augmentia.quad.core.agent.runtime.AgentEventPublisher;
import de.augmentia.quad.core.agent.runtime.IdempotencyStore;
import de.augmentia.quad.core.agent.runtime.OTelAgentTracer;
import de.augmentia.quad.core.agent.runtime.ToolExecutor;
import de.augmentia.quad.core.capability.context.ContextManager;
import de.augmentia.quad.core.capability.skill.Skill;
import de.augmentia.quad.core.capability.skill.SkillParser;
import de.augmentia.quad.core.capability.skill.TextSkill;
import de.augmentia.quad.core.config.StructuredOutputConfig;
import de.augmentia.quad.core.hook.pipeline.HookRegistry;
import de.augmentia.quad.core.hook.plugin.AgentSkillsPlugin;
import de.augmentia.quad.core.session.CurrentSession;
import de.augmentia.quad.core.session.WorkspaceResolver;
import de.augmentia.quad.core.tool.QuadToolRegistry;
import de.augmentia.quad.core.tool.SkillLoadTool;
import de.augmentia.quad.worker.config.AgentConfigSource;
import de.augmentia.quad.worker.config.ChatModelRegistry;
import de.augmentia.quad.worker.config.WorkerProperties;
import de.augmentia.quad.worker.tool.AgentToolRegistryFactory;
import de.augmentia.quad.worker.tool.ToolCatalog;
import dev.langchain4j.model.chat.ChatModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Factory that instantiates a completely new agent with all dependencies per task.
 *
 * Regeln:
 * - NO agent singleton (new instance per task)
 * - Pro Task neue QuadToolRegistry-Kopie
 * - Pro Task neue AgentSessionState
 * - Per-Thread CurrentSession via ThreadLocal-Cleanup
 */
public class WorkerAgentFactory {

    private static final Logger log = LoggerFactory.getLogger(WorkerAgentFactory.class);

    private final ChatModelRegistry models;
    private final AgentToolRegistryFactory registryFactory;
    private final CurrentSession currentSession;
    private final WorkspaceResolver workspaceResolver;
    private final IdempotencyStore idempotencyStore;
    private final OTelAgentTracer tracer;
    private final ToolCatalog toolCatalog;
    private final WorkerProperties properties;
    private final List<AgentConfigSource> sources;

    /** Parsed skills per (path, mtime) — avoids re-parsing per task. */
    private final Cache<String, List<Skill>> skillCache = Caffeine.newBuilder()
            .maximumSize(64)
            .expireAfterWrite(Duration.ofMinutes(10))
            .build();

    public WorkerAgentFactory(
        ChatModelRegistry models,
        AgentToolRegistryFactory registryFactory,
        CurrentSession currentSession,
        WorkspaceResolver workspaceResolver,
        IdempotencyStore idempotencyStore,
        OTelAgentTracer tracer,
        ToolCatalog toolCatalog,
        WorkerProperties properties
    ) {
        this(models, registryFactory, currentSession, workspaceResolver, idempotencyStore, tracer,
            toolCatalog, properties, List.of());
    }

    /**
     * Constructor with ordered config sources (DB-first, YAML-fallback).
     * Used by {@code QuadWorkerConfig}.
     */
    public WorkerAgentFactory(
        ChatModelRegistry models,
        AgentToolRegistryFactory registryFactory,
        CurrentSession currentSession,
        WorkspaceResolver workspaceResolver,
        IdempotencyStore idempotencyStore,
        OTelAgentTracer tracer,
        ToolCatalog toolCatalog,
        WorkerProperties properties,
        List<AgentConfigSource> sources
    ) {
        this.models = models;
        this.registryFactory = registryFactory;
        this.currentSession = currentSession;
        this.workspaceResolver = workspaceResolver;
        this.idempotencyStore = idempotencyStore;
        this.tracer = tracer;
        this.toolCatalog = toolCatalog;
        this.properties = properties;
        this.sources = sources == null ? List.of() : sources;
    }

    /**
     * Creates a fully configured agent based on agentType from WorkerProperties.
     * The lookup happens via properties.getAgents().get(agentType).
     */
    public Agent create(String agentType) throws Exception {
        // look up the agent config from WorkerProperties
        WorkerProperties.AgentConfig agentConfig = getAgentConfig(agentType);

        // Get the ChatModel (simple or advanced). get() already throws on an
        // unknown key — no additional null check needed.
        ChatModel model = models.get(agentConfig.modelKey());

        WorkerAgent agent = new WorkerAgent();

        // Per-task runtime dependencies (shared idempotency store + tracer).
        HookRegistry hooks = new HookRegistry();
        AgentEventPublisher events = new AgentEventPublisher();
        ToolExecutor toolExecutor = new ToolExecutor(idempotencyStore, tracer, events, hooks);

        // Catalog-only registry: the builder registers the agent's own tools, so
        // pre-registering them here would double-register.
        QuadToolRegistry registry = registryFactory.createCatalogOnly(agentConfig.allowedTools());

        // configuration (canonical quad-core config object)
        String systemPrompt = agentConfig.systemPrompt() != null && !agentConfig.systemPrompt().isBlank()
                ? agentConfig.systemPrompt() : defaultSystemPrompt();
        int maxReActIterations = agentConfig.maxReActIterations() != null
                ? agentConfig.maxReActIterations()
                : WorkerProperties.DEFAULT_MAX_REACT_ITERATIONS;

        AgentConfig config = AgentConfig.builder()
                .systemPrompt(systemPrompt)
                .maxReActIterations(maxReActIterations)
                .temperature(agentConfig.temperature())
                .maxOutputTokens(agentConfig.maxOutputTokens())
                .structuredOutput(agentConfig.structuredOutputEnabled()
                        ? StructuredOutputConfig.dynamicSchema(defaultJsonSchema())
                        : null)
                .build();

        WorkerAgent built = AgentBuilder.of(agent)
                .withModel(model)
                .withToolRegistry(registry)
                .withWorkspaceResolver(workspaceResolver)
                .withCurrentSession(currentSession)
                .withEventPublisher(events)
                .withHookRegistry(hooks)
                .withToolExecutor(toolExecutor)
                .withConfig(config)
                .build();

        built.setContextManager(new ContextManager());

        // ── Skill-Loading (JIT, Phase 1+2) ──
        registerSkillPlugins(built, agentConfig);

        log.info("Agent created for type={}, model={}", agentType, agentConfig.modelKey());
        return built;
    }

    /**
     * Registers Skill-Loading plugins and tools on the agent.
     *
     * <h3>Two-Phase Skill Loading (Just-in-Time)</h3>
     * <p><b>Phase 1 (Startup):</b> AgentSkillsPlugin injects a Skill-Catalog
     * (Name + Description only, 20-50 Tokens/Skill) into the system prompt.
     * <p><b>Phase 2 (Runtime):</b> The agent calls <code>load_skill("name")</code>
     * to load the full instructions as a Tool-Result — exactly like any other tool.
     *
     * <p>The directory is parsed per cache fingerprint, not per task:
     * {@code SkillParser.fromDirectory} re-reads and re-parses every {@code SKILL.md}.
     * The fingerprint from path plus mtime ensures that a change to the skills takes
     * effect without a restart.
     */
    private void registerSkillPlugins(WorkerAgent agent, WorkerProperties.AgentConfig config) {
        String skillsDir = config.skillsDir();
        if (skillsDir == null || skillsDir.isBlank()) return;

        Path dir = Path.of(skillsDir);
        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            log.warn("Skills directory not found: {} — skipping skill loading", skillsDir);
            return;
        }

        List<Skill> allSkills = loadSkills(dir);
        if (allSkills.isEmpty()) return;

        Map<String, Skill> skillMap = new HashMap<>();
        for (Skill s : allSkills) skillMap.put(s.name(), s);

        // Phase 1: AgentSkillsPlugin — injects only the Skill-Catalog (Name + Description)
        List<String> initialSkills = config.initialSkills() != null
            ? config.initialSkills() : List.of();
        var skillsPlugin = new AgentSkillsPlugin(allSkills, initialSkills, config.skillSearchEnabled());
        agent.addHook(skillsPlugin);

        // Phase 2: load_skill Tool — JIT loading of full instructions
        var loadSkillTool = new SkillLoadTool(skillMap);
        agent.getToolRegistry().register(loadSkillTool.spec().name(), loadSkillTool);

        log.debug("Skill-Loading enabled: {} skills loaded from {}", allSkills.size(), skillsDir);
    }

    /**
     * Reads and parses the skills directory, with a cache on (path, mtime).
     *
     * <p>The mtime of the directory changes when a file is added, deleted or renamed.
     * Changes <em>within</em> a SKILL.md file do not change the directory's mtime — for
     * content changes a restart of the worker is necessary.
     */
    private List<Skill> loadSkills(Path dir) {
        String key = skillCacheKey(dir);
        List<Skill> cached = skillCache.getIfPresent(key);
        if (cached != null) {
            return cached;
        }

        List<Skill> allSkills = new ArrayList<>();
        try {
            var parsed = SkillParser.fromDirectory(dir);
            for (var p : parsed) {
                allSkills.add(TextSkill.fromParseResult(p, dir.resolve(p.skillId())));
            }
            log.info("Parsed {} skills from {}", allSkills.size(), dir);
        } catch (Exception e) {
            log.warn("Failed to parse skills from {}: {}", dir, e.getMessage());
            return List.of();
        }

        if (allSkills.isEmpty()) return List.of();
        skillCache.put(key, allSkills);
        return allSkills;
    }

    private String skillCacheKey(Path dir) {
        long mtime;
        try {
            mtime = Files.getLastModifiedTime(dir).toMillis();
        } catch (Exception e) {
            mtime = 0L;
        }
        return dir.toAbsolutePath() + "@" + mtime;
    }

    /**
     * Gets the agent configuration from the ordered config sources
     * (by default: JDBC/agent_definitions first, YAML/application.yml as
     * fallback). If no source knows the agentType -> throw.
     */
    private WorkerProperties.AgentConfig getAgentConfig(String agentType) {
        for (AgentConfigSource source : sources) {
            Optional<WorkerProperties.AgentConfig> found = source.find(agentType);
            if (found.isPresent()) {
                log.debug("Agent config for type={} loaded from {}", agentType, source.name());
                return found.get();
            }
        }

        // Fallback: direct lookup from YAML (as before), for QUAD-only mode
        var agents = properties.agents();
        if (agents == null || agents.isEmpty()) {
            throw new IllegalStateException("No agents configured in worker.quad.agents");
        }
        
        WorkerProperties.AgentConfig config = agents.get(agentType);
        if (config == null) {
            // List the available agent types as a help
            String available = agents.keySet().stream()
                .sorted()
                .reduce((a, b) -> a + ", " + b)
                .orElse("(empty)");
            // The message used to list only the statically configured types. An agent that
            // exists in agent_definitions was reported as "unknown" with no hint, because
            // the DB is only consulted when worker.quad.agent-source=db — which is also the
            // required setting for anything imported through /agents/convert.
            throw new IllegalArgumentException("Unknown agent type: " + agentType +
                ". Available types: " + available +
                ". If this agent was registered via /agents/convert, set "
                + "worker.quad.agent-source=db — otherwise only the statically configured "
                + "types above are known.");
        }
        return config;
    }

    private String defaultSystemPrompt() {
        return """
            You are a helpful AI assistant working within a workflow system.
            Always return your results as structured JSON with the following fields:
            - response: your answer
            - confidence: a number between 0 and 1
            - status: COMPLETED or FAILED_RETRYABLE or FAILED_FATAL or NEEDS_HITL
            """;
    }
    
    private String defaultJsonSchema() {
        return """
            {
              "type": "object",
              "properties": {
                "response": {"type": "string"},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "status": {"type": "string", "enum": ["COMPLETED", "FAILED_RETRYABLE", "FAILED_FATAL", "NEEDS_HITL"]},
                "errorMessage": {"type": "string"}
              },
              "required": ["response"],
              "additionalProperties": true
            }
            """;
    }
}