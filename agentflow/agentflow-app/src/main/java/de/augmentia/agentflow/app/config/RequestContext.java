package de.augmentia.agentflow.app.config;

/**
 * Current run/session id for dynamic LLM headers ({@code {{requestId}}}/{@code {{sessionId}}}).
 *
 * <p>Set at the start of a run (before the first LLM call) and always cleared in a {@code finally}
 * block, because servlet/activity threads are reused.</p>
 */
public final class RequestContext {

    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

    private RequestContext() {
    }

    public static String current() {
        return CURRENT.get();
    }

    public static void set(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            CURRENT.remove();
        } else {
            CURRENT.set(requestId);
        }
    }

    public static void clear() {
        CURRENT.remove();
    }
}
