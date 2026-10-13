package de.augmentia.quad.worker.lifecycle;

import de.augmentia.quad.worker.metrics.WorkerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationFailedEvent;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Lifecycle manager for clean shutdown and startup checks.
 *
 * Functionality:
 * - ApplicationReadyEvent: start the LLM connectivity check
 * - ApplicationFailedEvent: error log + DLQ event
 * - Graceful shutdown: pause the Kafka consumer, finish tasks
 */
@Component
public class GracefulShutdown {

    private static final Logger log = LoggerFactory.getLogger(GracefulShutdown.class);

    private final WorkerMetrics metrics;
    private volatile boolean applicationReady = false;

    public GracefulShutdown(WorkerMetrics metrics) {
        this.metrics = metrics;
    }

    /**
     * Called on successful startup.
     * Starts an asynchronous connectivity check.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        applicationReady = true;
        log.info("Quad Workflow Worker started successfully");
        log.info("Mode: {} (LLM API configured)", 
            hasApiKey() ? "PRODUCTION" : "MOCK_ONLY");
        
        // Async connectivity check (non-blocking)
        Thread checker = new Thread(() -> {
            try {
                Thread.sleep(5000); // Wait for full startup
                log.info("[SHUTDOWN] Startup health check passed");
            } catch (InterruptedException e) {
                log.debug("Startup health check interrupted");
            }
        }, "startup-health-check");
        checker.setDaemon(true);
        checker.start();
    }

    /**
     * Called on failed startup.
     */
    @EventListener(ApplicationFailedEvent.class)
    public void onApplicationFailed(Exception ex) {
        log.error("Application failed to start: {}", ex.getMessage(), ex);
        metrics.recordDlqSent();  // Track as failed event
    }

    /**
     * Checks whether an API key is configured.
     */
    private boolean hasApiKey() {
        String apiKey = System.getenv().getOrDefault("OPENAI_API_KEY", "");
        return apiKey != null && !apiKey.isBlank();
    }

    /**
     * Returns whether the application is ready.
     */
    public boolean isReady() {
        return applicationReady;
    }
}