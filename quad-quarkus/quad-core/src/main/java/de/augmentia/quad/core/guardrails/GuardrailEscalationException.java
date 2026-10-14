package de.augmentia.quad.core.guardrails;

/**
 * Raised when a guardrail blocks a request with {@link BlockAction#ESCALATE}: the run cannot be
 * handled inline and must be routed to a human (HITL) approval flow.
 *
 * <p>Unlike {@link GuardrailException}, this is a routing signal: callers are expected to catch it
 * and start an escalation, not to treat it as a failure. Like {@code GuardrailException} it is
 * never swallowed by the hook pipeline (see {@code HookRegistry}).</p>
 */
public class GuardrailEscalationException extends RuntimeException {

    private final String guardrailReason;

    public GuardrailEscalationException(String guardrailReason) {
        super("Guardrail escalation: " + guardrailReason);
        this.guardrailReason = guardrailReason;
    }

    public String guardrailReason() {
        return guardrailReason;
    }
}
