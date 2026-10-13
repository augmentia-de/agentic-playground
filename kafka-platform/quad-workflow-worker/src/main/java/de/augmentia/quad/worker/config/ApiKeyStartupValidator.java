package de.augmentia.quad.worker.config;

import de.augmentia.agentplatform.common.event.ResultEvent;
import de.augmentia.quad.worker.kafka.WorkflowDlqPublisher;
import de.augmentia.quad.worker.kafka.WorkflowResultPublisher;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;

/**
 * Starts a validation of the LLM API settings at application start.
 * 
 * If API keys are missing or invalid:
 * - publish an error event to the Kafka DLQ
 * - a warning in the log
 */
public class ApiKeyStartupValidator {

    private static final Logger log = LoggerFactory.getLogger(ApiKeyStartupValidator.class);

    @Value("${OPENAI_API_KEY:}")
    private String apiKey;
    
    @Value("${OPENAI_BASE_URL:https://openrouter.ai/api/v1}")
    private String baseUrl;
    
    @Value("${OPENAI_MODEL:deepseek/deepseek-v4-flash-0731}")
    private String modelName;

    // fallback publisher (needed in the error case)
    private WorkflowResultPublisher resultPublisher;
    private WorkflowDlqPublisher dlqPublisher;

    public void setResultPublisher(WorkflowResultPublisher publisher) {
        this.resultPublisher = publisher;
    }

    public void setDlqPublisher(WorkflowDlqPublisher publisher) {
        this.dlqPublisher = publisher;
    }

    /**
     * Executed after the beans have been bootstrapped.
     * Validiert ob OPENAI_API_KEY gesetzt ist.
     */
    @PostConstruct
    public void validateApiKeys() {
        if (apiKey == null || apiKey.isBlank()) {
            log.error("OPENAI_API_KEY is not set! Worker runs in MOCK mode.");
            log.warn("No real LLM calls possible. Tasks are answered with mock results.");
            publishMockModeWarning();
        } else {
            log.info("LLM-API Key gefunden. Modell={}, URL={}", modelName, baseUrl);
            
            // Optional: brief connectivity check (non-blocking)
            validateConnectivityAsync();
        }
    }

    /**
     * Publishes a warning as an "error result" to the orchestrator.
     * That way the orchestrator knows that this worker cannot make real LLM calls.
     */
    private void publishMockModeWarning() {
        if (resultPublisher != null) {
            try {
                var warning = new ResultEvent(
                    new ResultEvent.Header(null, System.currentTimeMillis()),
                    "system", // Special workflow ID for system messages
                    "startup-validation",
                    "llm-api", 0, 0,
                    System.currentTimeMillis(), System.currentTimeMillis(),
                    Map.of(
                        "error", "No LLM API key configured",
                        "status", "FAILED_FATAL",
                        "workerMode", "MOCK_ONLY",
                        "message", "The worker cannot perform real LLM calls. Use .env with OPENAI_API_KEY."
                    )
                );
                resultPublisher.send("system", warning);
                log.info("Warning published: MOCK mode activated");
            } catch (Exception e) {
                log.error("Error while publishing the warning: {}", e.getMessage());
            }
        }
    }

    /**
     * Asynchrone Connectivity-Pruefung (blockiert keinen Startup).
     */
    private void validateConnectivityAsync() {
        Thread connectThread = new Thread(() -> {
            try {
                // short timeout test (1s) - only if the OpenRouter URL
                if (baseUrl.contains("openrouter")) {
                    java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder().build();
                    
                    java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                        .uri(java.net.URI.create(baseUrl + "/models"))
                        .header("Authorization", "Bearer " + apiKey)
                        .timeout(java.time.Duration.ofSeconds(1))
                        .GET()
                        .build();
                    
                    var response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() == 200) {
                        log.info("OpenRouter connectivity OK");
                    } else {
                        log.warn("OpenRouter connectivity check returned: {}", response.statusCode());
                    }
                }
            } catch (Exception e) {
                log.debug("Connectivity check skipped/failed (ok for dev): {}", e.getMessage());
            }
        }, "llm-connectivity-check");
        connectThread.setDaemon(true);
        connectThread.start();
    }
}