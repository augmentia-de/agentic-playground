package de.augmentia.quad.worker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.quad.core.agent.runtime.IdempotencyStore;
import de.augmentia.quad.core.agent.runtime.OTelAgentTracer;
import de.augmentia.quad.core.session.CurrentSession;
import de.augmentia.quad.core.session.WorkspaceResolver;
import de.augmentia.quad.core.tool.ToolArgsMapper;
import de.augmentia.quad.worker.agent.*;
import de.augmentia.quad.worker.kafka.*;
import de.augmentia.quad.worker.metrics.WorkerMetrics;
import de.augmentia.quad.worker.result.ResultMapper;
import de.augmentia.quad.worker.session.TaskSessionContext;
import de.augmentia.quad.worker.tool.AgentToolRegistryFactory;
import de.augmentia.quad.worker.tool.ToolCatalog;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Spring configuration that wires all worker beans.
 * 
 * Architecture:
 * - ToolCatalog: globally immutable, built from the built-in tools
 * - AgentToolRegistryFactory: erzeugt per-task Registry-Kopien
 * - WorkerAgentFactory: creates per-task agents with dependencies
 * - TaskExecutor: orchestrates task execution + result mapping
 */
@Configuration
@EnableConfigurationProperties(WorkerProperties.class)
public class QuadWorkerConfig {

    @Value("${worker.quad.group-id:quad-workflow-worker}")
    private String groupId;

    // ── Quad-core beans (manually wired, no CDI!) ──

    @Bean
    public CurrentSession quadCurrentSession() {
        return new CurrentSession();
    }

    @Bean(name = "quadObjectMapper")
    public ObjectMapper quadObjectMapper() {
        return new com.fasterxml.jackson.databind.ObjectMapper()
            .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
            .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Bean
    public ToolArgsMapper quadToolArgsMapper(@Qualifier("quadObjectMapper") ObjectMapper mapper) {
        return new ToolArgsMapper(mapper);
    }

    @Bean
    public WorkspaceResolver quadWorkspaceResolver(WorkerProperties props) {
        if (props.workspaceBase() != null && !props.workspaceBase().isBlank()) {
            System.setProperty("quad.workspace", props.workspaceBase());
        }
        return new WorkspaceResolver();
    }

    @Bean
    public IdempotencyStore quadIdempotencyStore() {
        return new IdempotencyStore();
    }

    @Bean
    public OTelAgentTracer quadOtelAgentTracer() {
        return new OTelAgentTracer();
    }

    // ── Builtin tools as a set for the ToolCatalog ──

    /**
     * All built-in tools are registered as individual beans.
     * The ToolCatalog scans these and builds the global catalog.
     */
    @Bean
    public de.augmentia.quad.core.tool.builtin.ReadFileTool readFileTool() {
        return new de.augmentia.quad.core.tool.builtin.ReadFileTool();
    }

    @Bean
    public de.augmentia.quad.core.tool.builtin.WriteTool writeTool() {
        return new de.augmentia.quad.core.tool.builtin.WriteTool();
    }

    @Bean
    public de.augmentia.quad.core.tool.builtin.GrepTool grepTool() {
        return new de.augmentia.quad.core.tool.builtin.GrepTool();
    }

    @Bean
    public de.augmentia.quad.core.tool.builtin.LsTool lsTool() {
        return new de.augmentia.quad.core.tool.builtin.LsTool();
    }

    @Bean
    public de.augmentia.quad.core.tool.builtin.FindTool findTool() {
        return new de.augmentia.quad.core.tool.builtin.FindTool();
    }

    @Bean
    public de.augmentia.quad.core.tool.builtin.WebSearchTool webSearchTool() {
        return new de.augmentia.quad.core.tool.builtin.WebSearchTool();
    }

    @Bean
    public de.augmentia.quad.core.tool.builtin.WebFetchTool webFetchTool() {
        return new de.augmentia.quad.core.tool.builtin.WebFetchTool();
    }

    // ── Tool Catalog ──

    @Bean
    public ToolCatalog toolCatalog(ToolArgsMapper argsMapper,
                                    de.augmentia.quad.core.tool.builtin.ReadFileTool readFileTool,
                                    de.augmentia.quad.core.tool.builtin.WriteTool writeTool,
                                    de.augmentia.quad.core.tool.builtin.GrepTool grepTool,
                                    de.augmentia.quad.core.tool.builtin.LsTool lsTool,
                                    de.augmentia.quad.core.tool.builtin.FindTool findTool,
                                    de.augmentia.quad.core.tool.builtin.WebSearchTool webSearchTool,
                                    de.augmentia.quad.core.tool.builtin.WebFetchTool webFetchTool) {
        Set<Object> builtinTools = Set.of(
            readFileTool, writeTool, grepTool, lsTool,
            findTool, webSearchTool, webFetchTool
        );
        return new ToolCatalog(argsMapper, builtinTools);
    }

    @Bean
    public AgentToolRegistryFactory agentToolRegistryFactory(ToolCatalog catalog, ToolArgsMapper argsMapper) {
        return new AgentToolRegistryFactory(catalog, argsMapper);
    }

    @Bean
    public ChatModelRegistry chatModelRegistry(org.springframework.core.env.Environment env) {
        String apiKey = env.getProperty("OPENAI_API_KEY", "");
        // Use fallback mechanism: if property not set or empty, use default URL
        String baseUrl = env.getProperty("OPENAI_BASE_URL");
        if (baseUrl == null || baseUrl.isEmpty()) {
            baseUrl = "https://openrouter.ai/api/v1";
        }
        String modelName = env.getProperty("OPENAI_MODEL");
        if (modelName == null || modelName.isEmpty()) {
            modelName = "deepseek/deepseek-v4-flash-0731";
        }

        // HTTP timeout of the provider. Must be larger than the task timeout that
        // the orchestrator sets (app.orchestration.max-task-timeout-ms, default 600 s).
        // Otherwise the HTTP call ends before the task, which ends as
        // FAILED_RETRYABLE — the operator then sees a retry instead of the
        // timeout and cannot attribute the cause. Previously a hard-wired
        // 120 s stood here, which silently ran every task with a larger timeout
        // into a retry.
        int llmTimeoutSeconds = parseIntWithDefault(
                firstNonBlank(env, "llm.timeout-seconds", "LLM_TIMEOUT_SECONDS"), 660);
        int maxTokens = parseIntWithDefault(
                firstNonBlank(env, "llm.max-tokens", "LLM_MAX_TOKENS"), 4096);
        double temperature = parseDoubleWithDefault(
                firstNonBlank(env, "llm.temperature", "LLM_TEMPERATURE"), 0.2);

        ChatModelRegistry registry = new ChatModelRegistry();

        // Fallback to mock when no API keys are set
        if (apiKey == null || apiKey.isBlank()) {
            org.slf4j.LoggerFactory.getLogger(ChatModelRegistry.class)
                .warn("No OPENAI_API_KEY found - using MOCK ChatModel");
            registry.register("simple", new ChatModelRegistry.MockChatModel());
            return registry;
        }

        try {
            // Echter OpenAI/OpenRouter ChatModel
            var builder = dev.langchain4j.model.openai.OpenAiChatModel.builder()
                .apiKey(apiKey)
                .modelName(modelName)
                .baseUrl(baseUrl)
                .timeout(Duration.ofSeconds(llmTimeoutSeconds))
                .maxTokens(maxTokens)
                .temperature(temperature);

        // OpenRouter needs custom headers for certain features
        var headers = new java.util.HashMap<String, String>();
        if (baseUrl.contains("openrouter")) {
            headers.put("HTTP-Referer", "https://github.com/augmentia-de/quad-workflow-worker");
            headers.put("X-Title", "quad-workflow-worker");
        }
        // Optional provider headers (e.g. OpenCode Go: User-Agent + x-opencode-session).
        // Empty unless QUAD_LLM_USER_AGENT / QUAD_LLM_SESSION_ID / QUAD_LLM_HEADERS is set.
        headers.putAll(de.augmentia.quad.core.config.ModelHeaders.fromEnv());
        if (!headers.isEmpty()) {
            builder.customHeaders(headers);
        }

        registry.register("simple", builder.build());
        
        org.slf4j.LoggerFactory.getLogger(ChatModelRegistry.class)
            .info("Real LLM configured: model={}, url={}", modelName, baseUrl);
        
        return registry;
        } catch (Exception ex) {
            org.slf4j.LoggerFactory.getLogger(ChatModelRegistry.class)
                .warn("Failed to configure real LLM (will use MOCK): {}", ex.getMessage());
            ChatModelRegistry mockRegistry = new ChatModelRegistry();
            mockRegistry.register("simple", new ChatModelRegistry.MockChatModel());
            return mockRegistry;
        }
}

    /**
     * First set property from a chain, {@code null} if none is set.
     * Allows inheritance from {@code application.yml} while at the same time
     * controlling via environment variable, as in the rest of the platform.
     */
    private static String firstNonBlank(org.springframework.core.env.Environment env, String... keys) {
        for (String key : keys) {
            String value = env.getProperty(key);
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private static int parseIntWithDefault(String raw, int fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            org.slf4j.LoggerFactory.getLogger(QuadWorkerConfig.class)
                .warn("Ungueltiger Zahlenwert '{}' — benutze Default {}", raw, fallback);
            return fallback;
        }
    }

    private static double parseDoubleWithDefault(String raw, double fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException e) {
            org.slf4j.LoggerFactory.getLogger(QuadWorkerConfig.class)
                .warn("Ungueltiger Zahlenwert '{}' — benutze Default {}", raw, fallback);
            return fallback;
        }
    }

    // ── AgentConfig sources (item 1): only with worker.quad.agent-source=db
    // A dedicated DataSource/JdbcTemplate instead of the Spring auto-config, so that
    // the worker stays startable without a database (QUAD-only, default yaml).
    @Bean
    @ConditionalOnProperty(prefix = "worker.quad", name = "agent-source", havingValue = "db")
    public javax.sql.DataSource quadAgentDataSource(
        org.springframework.core.env.Environment env
    ) {
        org.springframework.boot.jdbc.DataSourceBuilder<?> builder = org.springframework.boot.jdbc.DataSourceBuilder.create();
        String url = env.getProperty("spring.datasource.url");
        if (url == null || url.isBlank()) {
            throw new IllegalStateException(
                "worker.quad.agent-source=db erfordert spring.datasource.url "
                + "(z.B. jdbc:postgresql://localhost:5432/agent_platform)");
        }
        builder.url(url);
        String userName = env.getProperty("spring.datasource.username");
        String password = env.getProperty("spring.datasource.password");
        if (userName != null) builder.username(userName);
        if (password != null) builder.password(password);
        builder.driverClassName(env.getProperty("spring.datasource.driver-class-name", "org.postgresql.Driver"));
        return builder.build();
    }

    @Bean
    @ConditionalOnProperty(prefix = "worker.quad", name = "agent-source", havingValue = "db")
    public JdbcTemplate quadJdbcTemplate(javax.sql.DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    @ConditionalOnProperty(prefix = "worker.quad", name = "agent-source", havingValue = "db")
    public AgentConfigSource jdbcAgentConfigSource(
            JdbcTemplate jdbcTemplate,
            ObjectMapper quadObjectMapper,
            @org.springframework.beans.factory.annotation.Value(
                    "${worker.quad.react-max-iterations:10}") int defaultMaxReActIterations) {
        return new JdbcAgentConfigSource(jdbcTemplate, quadObjectMapper,
                java.time.Duration.ofSeconds(30), defaultMaxReActIterations);
    }

    @Bean
    public WorkerAgentFactory workerAgentFactory(
        ChatModelRegistry models,
        AgentToolRegistryFactory registryFactory,
        CurrentSession currentSession,
        WorkspaceResolver workspaceResolver,
        IdempotencyStore idempotencyStore,
        OTelAgentTracer tracer,
        ToolCatalog toolCatalog,
        WorkerProperties props,
        java.util.List<AgentConfigSource> agentConfigSources
    ) {
        return new WorkerAgentFactory(models, registryFactory, currentSession, workspaceResolver,
            idempotencyStore, tracer, toolCatalog, props, agentConfigSources);
    }

    // ── Execution Executor ──

    /**
     * Virtual-thread executor for LLM calls.
     *
     * <p>Deliberately <em>not</em> {@code ForkJoinPool.commonPool()}: its parallelism is
     * {@code nproc - 1}, whereas LLM calls are I/O-bound and need high parallelism
     * independent of the CPU count. Virtual threads cost almost nothing for blocking I/O;
     * the throughput limit is set solely by the permit semaphore in {@code TaskExecutor}
     * ({@code worker.quad.max-concurrent-tasks}).
     */
    @Bean(destroyMethod = "shutdown")
    public java.util.concurrent.ExecutorService quadAgentExecutor() {
        return java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
    }

    // ── Task Execution Pipeline ──

    @Bean
    public TaskPromptBuilder taskPromptBuilder() {
        return new TaskPromptBuilder();
    }

    @Bean
    public TaskSessionContext taskSessionContext(WorkerProperties props) {
        return new TaskSessionContext(props.workspaceBase());
    }

    @Bean
    public ResultMapper resultMapper() {
        return new ResultMapper();
    }

    // Kafka producer for results
    @Bean
    public Map<String, Object> producerConfig(org.springframework.core.env.Environment env) {
        Map<String, Object> config = new HashMap<>();
        // Same bootstrap source as the consumer -> a worker never consumes from one cluster
        // and produces to another. See KafkaConfig.resolveBootstrapServers.
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.resolveBootstrapServers(env));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        return config;
    }

    @Bean
    public org.springframework.kafka.core.ProducerFactory<String, String> producerFactory(Map<String, Object> producerConfig) {
        return new DefaultKafkaProducerFactory<>(producerConfig);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public org.apache.kafka.clients.producer.Producer<String, String> quadKafkaProducer(
            ProducerFactory<String, String> factory) {
        return factory.createProducer();
    }

    @Bean
    public WorkflowResultPublisher workflowResultProducer(Producer<String, String> producer,
                                                          WorkerProperties props,
                                                          org.springframework.core.env.Environment env) {
        return new WorkflowResultPublisher(producer, props.resultTopic(),
            env.getProperty("worker.quad.kafka.send-timeout-ms", Long.class, 30_000L));
    }

    @Bean
    public WorkflowDlqPublisher workflowDlqPublisher(Producer<String, String> producer,
                                                      WorkerProperties props,
                                                      org.springframework.core.env.Environment env) {
        return new WorkflowDlqPublisher(producer, props.dlqTopic(),
            env.getProperty("worker.quad.kafka.send-timeout-ms", Long.class, 30_000L));
    }

    @Bean
    public TaskExecutor taskExecutor(WorkerAgentFactory agentFactory,
                                      TaskPromptBuilder promptBuilder,
                                      TaskSessionContext sessionContext,
                                      ResultMapper resultMapper,
                                      WorkflowResultPublisher results,
                                      WorkflowDlqPublisher dlq,
                                      WorkerMetrics metrics,
                                      WorkerProperties props,
                                      @Qualifier("quadAgentExecutor") java.util.concurrent.ExecutorService agentExecutor) {
        return new TaskExecutor(agentFactory, promptBuilder, sessionContext, resultMapper, results, dlq,
            metrics, props.maxConcurrentTasks(), props.kafka().maxTaskTimeoutMs(), agentExecutor);
    }

    /**
     * Starts a validation of the LLM API settings at application start.
     * If API keys are missing -> publish an error event to the Kafka DLQ.
     */
    @Bean
    public ApiKeyStartupValidator apiKeyValidator(
            WorkerMetrics metrics,
            WorkflowResultPublisher results,
            WorkflowDlqPublisher dlq) {
        var validator = new ApiKeyStartupValidator();
        validator.setResultPublisher(results);
        validator.setDlqPublisher(dlq);
        return validator;
    }

    // NOTE: WorkerMetrics and GracefulShutdown are @Component-scanned, so they must NOT also
    // be declared as @Bean here — the duplicate definitions only worked because of
    // spring.main.allow-bean-definition-overriding, which is now disabled (see application.yml).
}