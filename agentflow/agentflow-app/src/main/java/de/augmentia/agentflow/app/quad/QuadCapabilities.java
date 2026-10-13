package de.augmentia.agentflow.app.quad;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.agentflow.app.observability.AgentTelemetry;
import de.augmentia.quad.core.agent.Agent;
import de.augmentia.quad.core.agent.AgentBuilder;
import de.augmentia.quad.core.config.ModelFactory;
import de.augmentia.quad.core.gdpr.AuditTrailHook;
import de.augmentia.quad.core.gdpr.PiiAnonymizerHook;
import de.augmentia.quad.core.guardrails.Guardrail;
import de.augmentia.quad.core.guardrails.GuardrailResult;
import de.augmentia.quad.core.observability.AgentMetrics;
import de.augmentia.quad.core.session.CurrentSession;
import de.augmentia.quad.core.session.WorkspaceResolver;
import de.augmentia.quad.core.tool.QuadToolRegistry;
import de.augmentia.quad.core.tool.ReflectiveToolMethod;
import de.augmentia.quad.core.tool.ToolArgsMapper;
import de.augmentia.quad.core.tool.builtin.FindTool;
import de.augmentia.quad.core.tool.builtin.GrepTool;
import de.augmentia.quad.core.tool.builtin.LsTool;
import de.augmentia.quad.core.tool.builtin.ReadFileTool;
import de.augmentia.quad.core.tool.builtin.WebFetchTool;
import de.augmentia.quad.core.tool.builtin.WebSearchTool;
import de.augmentia.quad.core.tool.builtin.WriteTool;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Deep reuse of quad-core capabilities beyond {@code Agents.simple()}:
 * built-in tools (with workspace confinement), guardrails, PII anonymization and an
 * audit trail — composed via the canonical {@link AgentBuilder}.
 *
 * <p>The model comes from {@link ModelFactory} and therefore also honours the optional
 * provider headers ({@code QUAD_LLM_USER_AGENT}/{@code QUAD_LLM_SESSION_ID}, e.g. OpenCode Go).</p>
 */
public final class QuadCapabilities {

    /** Built-in tool names (catalog names) available to agents. */
    public static final List<String> BUILTIN_TOOLS = List.of(
            "readFile", "writeFile", "listDirectory", "grepSearch", "findFiles", "webSearch", "webfetch");

    private QuadCapabilities() {
    }

    /**
     * Builds a fully-equipped quad-core agent.
     *
     * @param allowedTools  subset of {@link #BUILTIN_TOOLS}; empty/null = all built-ins
     * @param skillsDir     optional skills directory (JIT loading via {@code load_skill})
     * @param initialSkills skills active at start
     */
    public static Agent build(Set<String> allowedTools, Path skillsDir,
                              List<String> initialSkills, boolean skillSearch) {
        return build(allowedTools, skillsDir, initialSkills, skillSearch, null);
    }

    /**
     * Builds a fully-equipped quad-core agent, optionally recording {@code agent_*} metrics.
     *
     * @param allowedTools  subset of {@link #BUILTIN_TOOLS}; empty/null = all built-ins
     * @param skillsDir     optional skills directory (JIT loading via {@code load_skill})
     * @param initialSkills skills active at start
     * @param metrics       optional Micrometer metrics; when set the agent's event publisher
     *                      records LLM calls, tool executions, tokens and errors
     */
    public static Agent build(Set<String> allowedTools, Path skillsDir,
                              List<String> initialSkills, boolean skillSearch, AgentMetrics metrics) {
        WorkspaceResolver resolver = new WorkspaceResolver();

        AgentBuilder<de.augmentia.quad.core.agent.QuadAgent> builder = AgentBuilder.defaultAgent()
                .withModel(ModelFactory.createOpenAiFromEnv())
                .withToolRegistry(toolRegistry(allowedTools, resolver))
                .withWorkspaceResolver(resolver)
                .withCurrentSession(new CurrentSession())
                .withGuardrail(promptInjectionGuardrail())
                .withHook(piiHook())
                .withHook(new AuditTrailHook(new InMemoryAuditStore()))
                .withEventPublisher(AgentTelemetry.metricsPublisher(metrics))
                .withLogging();

        if (skillsDir != null) {
            builder = builder.withSkills(skillsDir)
                    .withInitialSkills(initialSkills != null ? initialSkills : List.of())
                    .withSkillSearch(skillSearch);
        }
        return builder.build();
    }

    /** Built-in tools registered under their catalog names, confined to the workspace. */
    public static QuadToolRegistry toolRegistry(Set<String> allowedTools, WorkspaceResolver resolver) {
        ToolArgsMapper mapper = new ToolArgsMapper(new ObjectMapper());
        QuadToolRegistry registry = new QuadToolRegistry(mapper);
        Map<String, Object> tools = builtinToolBeans(resolver);
        tools.forEach((name, bean) -> {
            if (allowedTools == null || allowedTools.isEmpty() || allowedTools.contains(name)) {
                register(registry, bean, name, mapper);
            }
        });
        return registry;
    }

    public static Map<String, Object> builtinToolBeans(WorkspaceResolver resolver) {
        Map<String, Object> tools = new LinkedHashMap<>();
        tools.put("readFile", withResolver(new ReadFileTool(), resolver));
        tools.put("writeFile", withResolver(new WriteTool(), resolver));
        tools.put("listDirectory", withResolver(new LsTool(), resolver));
        tools.put("grepSearch", withResolver(new GrepTool(), resolver));
        tools.put("findFiles", withResolver(new FindTool(), resolver));
        tools.put("webSearch", new WebSearchTool());
        tools.put("webfetch", new WebFetchTool());
        return tools;
    }

    private static Object withResolver(ReadFileTool tool, WorkspaceResolver r) { tool.setWorkspaceResolver(r); return tool; }
    private static Object withResolver(WriteTool tool, WorkspaceResolver r) { tool.setWorkspaceResolver(r); return tool; }
    private static Object withResolver(LsTool tool, WorkspaceResolver r) { tool.setWorkspaceResolver(r); return tool; }
    private static Object withResolver(GrepTool tool, WorkspaceResolver r) { tool.setWorkspaceResolver(r); return tool; }
    private static Object withResolver(FindTool tool, WorkspaceResolver r) { tool.setWorkspaceResolver(r); return tool; }

    private static void register(QuadToolRegistry registry, Object bean, String name, ToolArgsMapper mapper) {
        for (Class<?> c = bean.getClass(); c != null && c != Object.class; c = c.getSuperclass()) {
            for (Method m : c.getDeclaredMethods()) {
                if (m.getName().equals(name)) {
                    m.setAccessible(true);
                    registry.register(name, new ReflectiveToolMethod(m, bean, mapper));
                    return;
                }
            }
        }
    }

    /** Blocks obvious prompt-injection patterns on the input side. */
    public static Guardrail promptInjectionGuardrail() {
        return (messages, context) -> {
            String text = String.valueOf(messages).toLowerCase();
            if (text.contains("ignore all previous") || text.contains("jailbreak")
                    || text.contains("disregard the system")) {
                return GuardrailResult.block("Prompt-injection pattern detected");
            }
            return GuardrailResult.ok();
        };
    }

    /** Masks emails, phone numbers and credit cards before they reach the model. */
    public static PiiAnonymizerHook piiHook() {
        return new PiiAnonymizerHook(
                Set.of(PiiAnonymizerHook.MaskType.EMAIL,
                        PiiAnonymizerHook.MaskType.PHONE_NUMBER,
                        PiiAnonymizerHook.MaskType.CREDIT_CARD),
                PiiAnonymizerHook.BlockAction.REDACT,
                "[REDACTED]");
    }

    /** In-memory audit store (replace with a persistent one in production). */
    static final class InMemoryAuditStore implements AuditTrailHook.AuditStore {
        private final List<AuditTrailHook.AuditEntry> entries = new CopyOnWriteArrayList<>();

        @Override public void write(AuditTrailHook.AuditEntry entry) { entries.add(entry); }

        @Override public List<AuditTrailHook.AuditEntry> findByUserId(String userId) {
            return filter(e -> e.userId() != null && e.userId().equals(userId));
        }

        @Override public List<AuditTrailHook.AuditEntry> findBySessionId(String sessionId) {
            return filter(e -> e.sessionId() != null && e.sessionId().equals(sessionId));
        }

        @Override public List<AuditTrailHook.AuditEntry> findAll() { return List.copyOf(entries); }

        @Override public boolean verifyChain() { return true; }

        private List<AuditTrailHook.AuditEntry> filter(java.util.function.Predicate<AuditTrailHook.AuditEntry> p) {
            List<AuditTrailHook.AuditEntry> out = new ArrayList<>();
            for (AuditTrailHook.AuditEntry e : entries) if (p.test(e)) out.add(e);
            return out;
        }
    }
}
