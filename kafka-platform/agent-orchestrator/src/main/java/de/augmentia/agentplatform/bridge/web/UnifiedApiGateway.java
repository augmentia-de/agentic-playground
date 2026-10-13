package de.augmentia.agentplatform.bridge.web;

import de.augmentia.agentplatform.bridge.AgentDefinitionBridge;
import de.augmentia.agentplatform.bridge.AgentDefinitionBridge.WorkerAgentConfig;
import de.augmentia.agentplatform.bridge.WorkflowConversionException;
import de.augmentia.agentplatform.bridge.WorkflowDefinitionBridge;
import de.augmentia.agentplatform.bridge.QuarkusWorkflow;
import de.augmentia.agentplatform.common.model.DagNode;
import de.augmentia.agentplatform.orchestrator.config.KafkaTopicInitializer;
import de.augmentia.agentplatform.orchestrator.db.JdbcAgentDefinitionStore;
import de.augmentia.agentplatform.orchestrator.definition.WorkflowDefinitionUpload;
import de.augmentia.agentplatform.orchestrator.service.DuplicateWorkflowDefinitionException;
import de.augmentia.agentplatform.orchestrator.service.WorkflowDefinitionService;
import de.augmentia.agentplatform.orchestrator.service.WorkflowStartService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import tools.jackson.core.JacksonException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Unified API Gateway — routes frontend calls between Quarkus and Kafka backends.
 *
 * <p>{@code POST /workflows/from-quad} persists the converted workflow DIRECTLY into
 * the shared {@code workflow_definitions} table via the in-process
 * {@link WorkflowDefinitionService} (no HTTP round-trip to Quarkus) and auto-starts
 * the workflow on the Kafka orchestrator — so the UI works even when Quarkus is down.
 */
@Controller
@RequestMapping("/api/v1/unified")
public class UnifiedApiGateway {

    private static final Logger log = LoggerFactory.getLogger(UnifiedApiGateway.class);

    /**
     * Jackson 3, matching what Spring Boot 4 uses in the message converter.
     *
     * <p>Previously a dedicated Jackson 2 mapper stood here. The fat JAR contains both
     * Jackson generations, and the HTTP converter works with Jackson 3. A
     * {@code @RequestBody} on a <em>Jackson 2</em> {@code JsonNode} cannot be materialised
     * by this converter — every call to {@code /from-quad} failed with HTTP 500 and
     * {@code "Cannot construct instance of JsonNode (no Creators …)"}.
     */
    private static final tools.jackson.databind.ObjectMapper OBJECT_MAPPER =
            new tools.jackson.databind.json.JsonMapper();

    private final WebClient quarkusClient;
    private final WorkflowDefinitionService definitionService;
    private final WorkflowStartService startService;
    private final JdbcAgentDefinitionStore agentStore;
    private final int defaultMaxReActIterations;
    private final String defaultStubModelKey;
    private final ObjectProvider<KafkaTopicInitializer> topicInitializer;

    @org.springframework.beans.factory.annotation.Autowired
    public UnifiedApiGateway(
            // The default follows quarkus.http.port instead of a number of its own.
            //
            // Previously a fixed "http://localhost:8086" stood here. That property was
            // set nowhere, while the port of the quad application is controllable via
            // QUARKUS_HTTP_PORT. Anyone setting the port to 9090 silently got a 502 —
            // the gateway kept talking to 8086.
            //
            // A switch of its own (e.g. ${gateway.quad-url:...}) would have recreated
            // the same problem: a second name that nobody sets. The default now follows
            // the property that actually controls the port — relaxed binding thereby
            // also covers the QUARKUS_HTTP_PORT environment variable.
            @Value("${quarkus.base-url:http://localhost:${quarkus.http.port:8086}}") String quarkusBaseUrl,
            WorkflowDefinitionService definitionService,
            WorkflowStartService startService,
            JdbcAgentDefinitionStore agentStore,
            @Value("${app.agents.default-max-react-iterations:10}")
            int defaultMaxReActIterations,
            @Value("${app.agents.default-model-key:simple}")
            String defaultStubModelKey,
            ObjectProvider<KafkaTopicInitializer> topicInitializer) {
        this.quarkusClient = WebClient.builder()
                .baseUrl(quarkusBaseUrl)
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
        this.definitionService = definitionService;
        this.startService = startService;
        this.agentStore = agentStore;
        // quad may set the limit itself; if it does not, the orchestrator's
        // configuration decides, not a value in the code.
        this.defaultMaxReActIterations = defaultMaxReActIterations > 0
                ? defaultMaxReActIterations
                : AgentDefinitionBridge.DEFAULT_MAX_REACT_ITERATIONS;
        // Must be a key the worker has registered; see JdbcAgentDefinitionStore.ensureStub.
        this.defaultStubModelKey = defaultStubModelKey;
        this.topicInitializer = topicInitializer;
    }

    /**
     * Convenience constructor without topic provisioning, for callers outside Spring.
     *
     * <p>Not annotated: the class now has two constructors, so Spring needs to be told which
     * one to inject. Without {@code @Autowired} on the primary it looks for a no-arg
     * constructor, does not find one and the context fails to start.
     */
    public UnifiedApiGateway(String quarkusBaseUrl, WorkflowDefinitionService definitionService,
                             WorkflowStartService startService, JdbcAgentDefinitionStore agentStore,
                             int defaultMaxReActIterations, String defaultStubModelKey) {
        this(quarkusBaseUrl, definitionService, startService, agentStore,
                defaultMaxReActIterations, defaultStubModelKey, null);
    }

    /**
     * Convert & save a quad-ui flat workflow into shared Kafka storage, then start it.
     *
     * <p>Response: {@code {converted, nodeCount, originalName, kafkaId, name, version,
     * executionId, duplicate}} where {@code executionId} is the running workflow execution.
     */
    @PostMapping(value = "/workflows/from-quad", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> convertAndSaveQuadWorkflow(
            @RequestBody tools.jackson.databind.JsonNode quadPayload,
            @RequestHeader(name = "X-Backend", defaultValue = "kafka") String backend,
            @RequestHeader(name = "Idempotency-Key", required = false) String idempotencyKey) {

        if ("quarkus".equalsIgnoreCase(backend)) {
            return doQuarkusProxyPost("/api/ui/workflows", quadPayload);
        }

QuarkusWorkflow qwf;
        List<DagNode> dagNodes;
        try {
            qwf = OBJECT_MAPPER.treeToValue(quadPayload, QuarkusWorkflow.class);
            dagNodes = WorkflowDefinitionBridge.convert(qwf);
        } catch (WorkflowConversionException e) {
            // Expected result of a rejectable payload, not an operational error: the
            // caller sent something the Kafka orchestrator cannot execute (unknown node
            // type, LOOP without a reachable body). That does not belong in the log as
            // ERROR with a stacktrace — otherwise the regular 400 path disturbs
            // troubleshooting because the line looks like an outage.
            log.debug("Bridge rejected quad workflow '{}': {}", quadPayload.path("name").asText("(without name)"),
                    e.getMessage());
            return ResponseEntity.badRequest().body(Map.of("error", "Conversion failed: " + e.getMessage()));
        } catch (JacksonException e) {
            // Unparseable payload — that too is an error of the caller, not a 500.
            log.debug("Bridge could not read quad payload: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of("error", "Malformed payload: " + e.getMessage()));
        }

        if (dagNodes.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "No convertible nodes in workflow"));
        }

        // From here on it is our error, not the caller's: DB, broker, Redis.
        try {
            // 1) Persist converted DAG into the SHARED workflow_definitions table (in-process).
            WorkflowDefinitionUpload upload = toUpload(qwf.name(), dagNodes);
            UUID definitionId;
            boolean duplicate = false;
            try {
                definitionId = definitionService.create(upload);
            } catch (DuplicateWorkflowDefinitionException ex) {
                duplicate = true;
                definitionId = definitionService.requireForStart(qwf.name(), 1).id();
            }

            // 2) Auto-register referenced agents so Kafka workers can resolve them, and
            // provision their task topics. Provisioning here rather than leaving it to the
            // broker matters: the broker creates an unknown topic on first produce with its
            // default of ONE partition, which serialises that agent type while every other
            // type runs on 12.
            java.util.List<String> agentTypes = dagNodes.stream()
                    .map(DagNode::agentType)
                    .filter(agentType -> agentType != null && !agentType.isBlank())
                    .distinct()
                    .toList();
            agentTypes.forEach(agentType -> agentStore.ensureStub(agentType, defaultStubModelKey));
            // Null outside Spring, where the convenience constructor passes no provider.
            KafkaTopicInitializer topics = topicInitializer == null
                    ? null : topicInitializer.getIfAvailable();
            if (topics != null && !agentTypes.isEmpty()) {
                try {
                    topics.ensureTopicsFor(agentTypes);
                } catch (Exception e) {
                    // The broker may create the topic on demand; the partition count would then
                    // be short, which is a performance problem, not a reason to reject the import.
                    log.warn("Could not provision task topics for {}: {}", agentTypes, e.toString());
                }
            }

            // 3) Start the workflow on the Kafka orchestrator right away.
            String key = resolveIdempotencyKey(idempotencyKey);
            WorkflowStartService.StartResult started = startService.start(
                    qwf.name(), 1, Map.of("task", initialTaskOf(quadPayload)), key);

            log.info("Converted+saved+started quad workflow '{}' → {} Kafka DAG nodes, execution {}",
                    qwf.name(), dagNodes.size(), started.executionId());
            return ResponseEntity.status(duplicate ? 200 : 202).body(Map.of(
                    "converted", true, "nodeCount", dagNodes.size(), "originalName", qwf.name(),
                    "kafkaId", definitionId.toString(), "name", qwf.name(), "version", 1,
                    "executionId", started.executionId().toString(), "duplicate", started.duplicate(),
                    // Echoed back so a client whose request timed out can repeat it with this
                    // key and receive the same execution instead of starting a second one.
                    "idempotencyKey", key
            ));

        } catch (Exception e) {
            // 400 would be a false report here — the client cannot fix this with another
            // request, and troubleshooting would search at the caller.
            log.error("Failed to save/start quad workflow '{}'", qwf.name(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Conversion failed: " + e.getMessage()));
        }
    }

    /** Proxy GET call to Quarkus. */
    @GetMapping("/quarkus/{*path}")
    public ResponseEntity<?> proxyRestCall(@PathVariable String path) {
        return doQuarkusProxyGet("/api/ui/" + path);
    }

    /** Proxy POST call to Quarkus. */
    @PostMapping("/quarkus/{*path}")
    public ResponseEntity<?> proxyPostCall(@PathVariable String path,
                                           @RequestBody(required = false) tools.jackson.databind.JsonNode body) {
        return doQuarkusProxyPost("/api/ui/" + path, body);
    }

    /** SSE stream proxy — forwards checkpoint events from Quarkus. */
    @GetMapping("/quarkus/stream/{*path}")
    public SseEmitter proxySseStream(@PathVariable String path) {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);

        quarkusClient.get().uri("/api/ui/" + path).accept(MediaType.TEXT_EVENT_STREAM).retrieve()
                .toEntity(String.class).subscribe(
                        resp -> {
                            if (resp.getStatusCode().is2xxSuccessful()) {
                                try { emitter.send(SseEmitter.event().name("data").data(resp.getBody())); }
                                catch (IOException e) { emitter.completeWithError(e); }
                            } else { emitter.completeWithError(new IOException("Status " + resp.getStatusCode())); }
                        },
                        err -> emitter.completeWithError(err)
                );
        return emitter;
    }

    /** Merge metrics from both backends. */
    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> mergeMetrics() {
        return ResponseEntity.ok(Map.of(
                "kafkaOrchestrator", fetchMetricSource("/actuator/prometheus"),
                "quadQuarkus", fetchMetricSource("/api/ui/metrics"),
                "merged", Map.of("status", "ok", "timestamp", System.currentTimeMillis())
        ));
    }

    /** Convert a Quarkus-rich AgentDefinition into a Kafka WorkerAgentConfig & register it. */
    @PostMapping(value = "/agents/convert", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> convertAgent(@RequestBody tools.jackson.databind.JsonNode agentPayload) {
        AgentDefinitionBridge.QuarkusAgent qa;
        WorkerAgentConfig config;
        try {
            qa = OBJECT_MAPPER.treeToValue(agentPayload, AgentDefinitionBridge.QuarkusAgent.class);
            config = AgentDefinitionBridge.fromQuadAgent(qa, defaultMaxReActIterations);
        } catch (JacksonException e) {
            // Unparseable agent payload — an error of the caller.
            log.debug("Bridge could not read agent payload: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of("error", "Malformed payload: " + e.getMessage()));
        }

        String agentName = (config.agentName() != null && !config.agentName().isBlank())
                ? config.agentName() : config.agentId();

        // From here on it is our error: agentStore writes into the shared table, and an
        // outage there is not a client error. 400 would send troubleshooting to the
        // Aufrufer schicken, wo nichts zu reparieren ist.
        try {
            // Both values are taken over unchanged. max_tokens is a token budget and
            // lands in the worker in paramsBuilder.maxOutputTokens(...);
            // max_react_iterations is the agent's loop limit.
            agentStore.upsert(
                    config.agentId(),
                    agentName,
                    config.systemPrompt(),
                    config.modelKey(),
                    config.temperature(),
                    config.maxOutputTokens(),
                    config.timeoutSeconds(),
                    config.maxRetryAttempts(),
                    config.version(),
                    config.maxReActIterations());
        } catch (Exception e) {
            log.error("Failed to register converted agent '{}'", config.agentId(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Registration failed: " + e.getMessage()));
        }

        return ResponseEntity.ok(Map.of(
                "converted", true, "agentId", config.agentId(), "modelKey", config.modelKey(),
                "allowedTools", List.copyOf(config.allowedTools()),
                "registered", true,
                "warnings", List.of(
                        "Skills registry integration lost in conversion",
                        "Named hooks (pre/post) lost in conversion",
                        "Directory permissions lost in conversion",
                        "MCP/Sandbox support not available in Kafka worker"
                )
        ));
    }

    // ── Private helpers ──────────────────────────────────────

    private static WorkflowDefinitionUpload toUpload(String name, List<DagNode> dagNodes) {
        List<WorkflowDefinitionUpload.DagNodeUpload> nodes = dagNodes.stream()
                .map(n -> new WorkflowDefinitionUpload.DagNodeUpload(
                        n.stepId(),
                        n.nodeType().name(),
                        n.agentType(),
                        null,
                        n.requiresHumanApproval(),
                        n.dependsOn(),
                        n.guardCondition(),
                        n.branches() == null ? List.of() : n.branches().stream()
                                .map(b -> new WorkflowDefinitionUpload.BranchUpload(
                                        b.branchId(), b.condition(), b.target(), b.firstStep()))
                                .toList(),
                        n.defaultTarget(),
                        n.joinType(),
                        n.joinBranches(),
                        n.joinN(),
                        n.maxIterations(),
                        n.exitCondition(),
                        n.timeoutMs(),
                        n.bodySteps()
                )).toList();
        return new WorkflowDefinitionUpload(name, 1, new WorkflowDefinitionUpload.DagStructure(nodes));
    }

    private static String initialTaskOf(tools.jackson.databind.JsonNode payload) {
        var task = payload.get("task");
        return task != null && task.isTextual() ? task.asText() : "Run workflow";
    }

    /**
     * The caller's key, or a fresh one.
     *
     * <p>This endpoint both converts and starts, so a client that retries after a timeout
     * would otherwise start the workflow twice. Previously a random key was generated per
     * request and never returned, which made {@code WorkflowStartService}'s idempotency
     * unreachable from here: the DB check, the guard and the UNIQUE constraint could never
     * match anything.
     *
     * <p>No key is derived from the payload on purpose — running the same workflow twice in
     * a row is a legitimate request, not a retry. With no key a fresh execution starts; with
     * the key from the previous response the retry resolves to the existing execution.
     */
    private static String resolveIdempotencyKey(String supplied) {
        if (supplied != null && !supplied.isBlank()) return supplied.trim();
        return "quad-" + UUID.randomUUID();
    }

    /** Forward a GET call to Quarkus. */
    private ResponseEntity<?> doQuarkusProxyGet(String path) {
        try {
            var resp = quarkusClient.get().uri(path).retrieve().toBodilessEntity().block();
            if (resp == null) throw new RuntimeException("No response");
            return ResponseEntity.ok(Map.of("proxied", true, "status", resp.getStatusCode().value()));
        } catch (Exception e) {
            log.warn("GET proxy to Quarkus failed: {}", e.getMessage());
            return new ResponseEntity<>(Map.of("error", "Proxy failed: " + e.getMessage()), HttpStatus.BAD_GATEWAY);
        }
    }

    /** Forward a POST call to Quarkus. */
    private ResponseEntity<Map<String, Object>> doQuarkusProxyPost(String path, tools.jackson.databind.JsonNode body) {
        String jsonBody = body != null ? OBJECT_MAPPER.writeValueAsString(body) : "{}";
        // exchangeToMono instead of retrieve(): retrieve() throws on 4xx/5xx, so the
        // workflow came back as a 502 "Proxy failed" — the caller took its rejection
        // for an outage of the proxy.
        //
        // The body arrives as a String, not a JsonNode: an error page often delivers HTML
        // or no content type at all, and a type conversion on that makes the proxy fail on
        // the target's response instead of passing it through.
        return quarkusClient.post().uri(path)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(jsonBody)
                .exchangeToMono(response -> response.bodyToMono(String.class).defaultIfEmpty("{}")
                        .map(raw -> {
                            Map<String, Object> result = new java.util.LinkedHashMap<>();
                            result.put("proxied", true);
                            result.put("status", response.statusCode().value());
                            try {
                                result.put("data", OBJECT_MAPPER.readValue(raw, Map.class));
                            } catch (RuntimeException notJson) {
                                // Not JSON — the content passes through as text. The
                                // caller sees the target's error message, not a proxy
                                // outage.
                                result.put("data", raw);
                            }
                            return ResponseEntity.status(response.statusCode()).<Map<String, Object>>body(result);
                        }))
                .onErrorResume(e -> reactor.core.publisher.Mono.just(
                        new ResponseEntity<>(Map.of("error", "Proxy failed: " + e.getMessage()),
                                HttpStatus.BAD_GATEWAY)))
                .block();
    }

    private Map<String, Object> fetchMetricSource(String uri) {
        try {
            quarkusClient.get().uri(uri).retrieve().toBodilessEntity().block();
            return Map.of("source", "detected", "available", true);
        } catch (Exception e) {
            return Map.of("source", "unknown", "available", false, "error", e.getMessage());
        }
    }
}