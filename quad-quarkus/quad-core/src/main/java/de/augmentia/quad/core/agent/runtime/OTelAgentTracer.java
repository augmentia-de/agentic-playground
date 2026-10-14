package de.augmentia.quad.core.agent.runtime;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class OTelAgentTracer {
    private volatile Tracer tracer;

    @Inject
    public OTelAgentTracer(Tracer tracer) {
        this.tracer = tracer;
    }

    /** Required no-arg constructor for CDI (e.g. tests, non-Quarkus contexts) */
    public OTelAgentTracer() {
    }

    public boolean isEnabled() {
        return tracer != null;
    }

    public Tracer getTracer() {
        return tracer;
    }

    public void trace(String event, Object... args) {
        var current = tracer;
        if (current == null) return;
        Span span = Span.current();
        span.addEvent(event);
        if (args != null) {
            for (int i = 0; i + 1 < args.length; i += 2) {
                span.setAttribute(String.valueOf(args[i]), String.valueOf(args[i + 1]));
            }
        }
    }

    public void setTracer(Tracer tracer) {
        this.tracer = tracer;
    }

    public Span startToolSpan(String toolName, String sessionId, String arguments) {
        var current = tracer;
        if (current == null) return null;
        Span span = current.spanBuilder(toolName)
                .setAttribute("tool.name", toolName)
                .setAttribute("session.id", sessionId)
                .setAttribute("tool.args", arguments)
                .startSpan();
        span.addEvent("tool.started");
        return span;
    }

    public void endToolSpan(Span span, String result, long duration) {
        if (span == null) return;
        span.setAttribute("tool.duration_ms", duration);
        span.setAttribute("tool.result", result != null ? result.substring(0, Math.min(result.length(), 1000)) : "null");
        span.addEvent("tool.finished");
        span.end();
    }

    public void failToolSpan(Span span, Exception error, long duration) {
        if (span == null) return;
        span.setAttribute("tool.duration_ms", duration);
        span.setStatus(StatusCode.ERROR, error.getMessage());
        span.recordException(error);
        span.end();
    }

    /**
     * Adds a span event, tagging it with the tool name when one is given (the tool identity was
     * previously dropped, so events were unattributable).
     */
    public void addEvent(String eventName, String toolName) {
        if (tracer == null) return;
        addEvent(Span.current(), eventName, toolName);
    }

    /** Package-private seam so tests can assert the tool name lands on the span. */
    void addEvent(Span span, String eventName, String toolName) {
        if (toolName == null || toolName.isBlank()) {
            span.addEvent(eventName);
        } else {
            span.addEvent(eventName, Attributes.of(AttributeKey.stringKey("tool.name"), toolName));
        }
    }
}
