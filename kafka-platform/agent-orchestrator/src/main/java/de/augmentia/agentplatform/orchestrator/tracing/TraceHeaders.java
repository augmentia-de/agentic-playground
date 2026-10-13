package de.augmentia.agentplatform.orchestrator.tracing;

import io.micrometer.tracing.Span;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds W3C trace-context headers for outgoing Kafka messages.
 *
 * <p><b>Why no {@code ProducerInterceptor}?</b> Kafka instantiates classes
 * registered in {@code interceptor.classes} reflectively via a
 * <em>no-arg constructor</em> — without a Spring context. A Spring bean with
 * constructor injection fundamentally cannot be created there; the
 * attempt failed with {@code Could not find a public no-argument
 * constructor}, and because producer creation failed, the
 * orchestrator could publish <em>nothing</em>.
 *
 * <p>The headers are therefore created directly at publish, where the
 * {@code Tracer} is available as a bean. Tracing remains optional: without an active span
 * simply no header is set.
 */
public final class TraceHeaders {

    private static final Logger log = LoggerFactory.getLogger(TraceHeaders.class);

    /** W3C traceparent: 00-{32-hex-traceId}-{16-hex-spanId}-flags */
    public static final String TRACEPARENT = "traceparent";
    public static final String CORRELATION_ID = "x-correlation-id";

    private TraceHeaders() {}

    /**
     * Attaches {@code traceparent} and {@code x-correlation-id} to the existing
     * headers. Without an active span or without extractable IDs
     * {@code headers} is returned unchanged.
     */
    public static List<Header> enrich(Span currentSpan, String correlationId, List<Header> existing) {
        List<Header> result = new ArrayList<>();
        if (existing != null) {
            result.addAll(existing);
        }

        String traceparent = traceparent(currentSpan);
        if (traceparent == null) {
            return result;
        }

        result.add(new RecordHeader(TRACEPARENT, traceparent.getBytes(StandardCharsets.UTF_8)));
        if (correlationId != null) {
            result.add(new RecordHeader(CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8)));
        }
        return result;
    }

    /**
     * Formats the W3C traceparent of a span, or {@code null} if the span
     * delivers no usable IDs.
     */
    public static String traceparent(Span currentSpan) {
        if (currentSpan == null) {
            return null;
        }
        try {
            Object ctxObj = safeInvoke(currentSpan, "context");
            if (ctxObj == null) {
                return null;
            }
            String traceId = asString(safeInvoke(ctxObj, "traceId"));
            String spanId = asString(safeInvoke(ctxObj, "spanId"));
            if (traceId == null || spanId == null) {
                return null;
            }
            boolean sampled = isSampled(ctxObj);
            String traceIdHex = padTo(traceId, 32);
            String spanIdHex = padTo(spanId, 16);
            return "00-" + traceIdHex + "-" + spanIdHex + "-" + (sampled ? "01" : "00");
        } catch (Exception e) {
            log.debug("Failed to build traceparent: {}", e.getMessage());
            return null;
        }
    }

    private static String padTo(String value, int length) {
        String v = value.toLowerCase();
        if (v.length() > length) {
            return v.substring(v.length() - length);
        }
        StringBuilder sb = new StringBuilder(length);
        for (int i = v.length(); i < length; i++) {
            sb.append('0');
        }
        return sb.append(v).toString();
    }

    private static Object safeInvoke(Object target, String methodName) throws Exception {
        if (target == null) {
            return null;
        }
        Method chosen = null;
        for (Method m : target.getClass().getMethods()) {
            if (m.getName().equals(methodName) && m.getParameterCount() == 0) {
                if (chosen == null
                        || chosen.getReturnType().isAssignableFrom(m.getReturnType())) {
                    chosen = m;
                }
            }
        }
        if (chosen == null) {
            return null;
        }
        chosen.setAccessible(true);
        return chosen.invoke(target);
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }

    private static boolean isSampled(Object ctxObj) throws Exception {
        Object bool = safeInvoke(ctxObj, "isSampled");
        if (bool instanceof Boolean b) {
            return b;
        }
        Object number = safeInvoke(ctxObj, "sampled");
        if (number instanceof Number n) {
            return n.intValue() != 0;
        }
        return false;
    }
}
