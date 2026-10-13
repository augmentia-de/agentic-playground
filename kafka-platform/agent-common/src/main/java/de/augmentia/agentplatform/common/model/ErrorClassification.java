package de.augmentia.agentplatform.common.model;

/**
 * Error classification for retry/escalation decisions (§9.5).
 */
public enum ErrorClassification {
    /** Transient error — may resolve on retry. */
    TRANSIENT,
    /** Business logic error — requires human decision. */
    BUSINESS,
    /** Fatal error — unrecoverable, send to DLQ. */
    FATAL;

    public boolean isRetryable() { return this == TRANSIENT; }
    public boolean needsHitlEscalation() { return this == BUSINESS; }
    public boolean isTerminal() { return this == FATAL; }

    /** Classify an error message string using keyword matching heuristics. */
    public static ErrorClassification classify(String errorMessage) {
        if (errorMessage == null || errorMessage.isBlank()) return FATAL;
        String lower = errorMessage.toLowerCase();

        // Terminal checks first (most specific)
        if (lower.contains("invalid step id") || lower.contains("missing config") ||
            lower.contains("malformed") || lower.contains("data corrupted")) {
            return FATAL;
        }
        // Transient checks
        if (lower.contains("timeout") || lower.contains("rate limit") || lower.contains("throttl") ||
            lower.contains("circuit break") || lower.contains("network error") || lower.contains("connection refused")) {
            return TRANSIENT;
        }
        // Business checks
        if (lower.contains("guardrail") || lower.contains("violation") || lower.contains("confidence") ||
            lower.contains("approval") || lower.contains("unexpected format")) {
            return BUSINESS;
        }
        // Default: HTTP 4xx → business, others → transient
        return lower.contains("403") || lower.contains("forbidden") || lower.contains("validation") ? BUSINESS : TRANSIENT;
    }

    /** Classify from an exception class name. */
    public static ErrorClassification classifyFromException(Class<? extends Throwable> exceptionClass) {
        String className = exceptionClass.getSimpleName().toLowerCase();
        if (className.contains("illegalstate") || className.contains("illegalargument") ||
            className.contains("validationexception")) {
            return FATAL;
        }
        if (className.contains("timeout") || className.contains("socket") ||
            className.contains("sslhandshake") || className.contains("connectexception")) {
            return TRANSIENT;
        }
        if (className.contains("securityexception") || className.contains("permissiondenied")) {
            return BUSINESS;
        }
        return TRANSIENT; // Default safe choice
    }

    /** Classify from an error message pattern (for programmatic use). */
    public static ErrorClassification classify(String errorMessage, boolean isRetryAttempt) {
        if (errorMessage == null || errorMessage.isBlank()) return FATAL;
        String lower = errorMessage.toLowerCase();

        // Terminal checks first (most specific)
        if (lower.contains("invalid step id") || lower.contains("missing config") ||
            lower.contains("malformed") || lower.contains("data corrupted") ||
            lower.contains("schema validation failed") || lower.contains("unknown node type")) {
            return FATAL;
        }

        // Transient checks — network/system level failures
        if (lower.contains("timeout") || lower.contains("rate limit") || lower.contains("throttl") ||
            lower.contains("circuit break") || lower.contains("network error") || 
            lower.contains("connection refused") || lower.contains("server error 5") ||
            lower.contains("backoff") || lower.contains("temporary") ||
            lower.contains("econnrefused") || lower.contains("sslhandshake") ||
            lower.contains("tls handshake failed")) {
            return TRANSIENT;
        }

        // Business checks
        if (lower.contains("guardrail") || lower.contains("violation") || 
            lower.contains("confidence") || lower.contains("approval") ||
            lower.contains("unexpected format") || lower.contains("content policy") ||
            lower.contains("unsafe output") || lower.contains("pii detected")) {
            return BUSINESS;
        }

        // HTTP status code classification
        if (lower.contains("429") || lower.contains("too many requests")) return TRANSIENT;
        if (lower.contains("403") || lower.contains("forbidden")) return BUSINESS;
        if (lower.contains("503") || lower.contains("unavailable")) return TRANSIENT;
        if (lower.contains("504") || lower.contains("gateway timeout")) return TRANSIENT;

        // Retry attempt heuristic: if retry fails again, escalate
        if (isRetryAttempt && (lower.contains("still failing") || lower.contains("exhausted"))) {
            return FATAL;
        }

        // Default: HTTP 4xx → business, everything else → transient
        return lower.contains("403") || lower.contains("forbidden") || 
               lower.contains("validation") ? BUSINESS : TRANSIENT;
    }
}
