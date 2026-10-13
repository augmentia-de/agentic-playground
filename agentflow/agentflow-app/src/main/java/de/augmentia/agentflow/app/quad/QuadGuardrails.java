package de.augmentia.agentflow.app.quad;

import de.augmentia.quad.core.gdpr.AuditTrailHook;
import de.augmentia.quad.core.gdpr.PiiAnonymizerHook;
import de.augmentia.quad.core.guardrails.Guardrail;
import de.augmentia.quad.core.guardrails.GuardrailResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Named guardrails and hooks for <b>declarative</b> agents.
 *
 * <p>Agent documents carry {@code guardrailsInput}/{@code guardrailsOutput} (list of names) and
 * {@code hooks} (list of specs). This class resolves those strings to real quad-core
 * implementations, mirroring the naming used by quad-quarkus ({@code GuardrailFactory}):
 * {@code pii}, {@code sql-injection}, {@code prompt-injection}. Hook specs use
 * {@code name[:arg1,arg2]} — e.g. {@code pii} / {@code pii:EMAIL,PHONE_NUMBER} (masking) and
 * {@code audit}.</p>
 */
public final class QuadGuardrails {

    private static final Logger log = LoggerFactory.getLogger(QuadGuardrails.class);

    /** Guardrail names selectable for {@code guardrailsInput}/{@code guardrailsOutput}. */
    public static final List<String> GUARDRAIL_NAMES = List.of("pii", "sql-injection", "prompt-injection");

    /** Hook names selectable for {@code hooks} (spec form {@code name[:args]}). */
    public static final List<String> HOOK_NAMES = List.of("pii", "audit");

    /** PII mask types for the {@code pii} hook. */
    public static final List<String> PII_MASK_TYPES =
            List.of("EMAIL", "PHONE_NUMBER", "NAME_DE", "CREDIT_CARD", "ADDRESS");

    private static final Set<PiiAnonymizerHook.MaskType> DEFAULT_MASKS = Set.of(
            PiiAnonymizerHook.MaskType.EMAIL,
            PiiAnonymizerHook.MaskType.PHONE_NUMBER,
            PiiAnonymizerHook.MaskType.CREDIT_CARD);

    private static final Pattern EMAIL =
            Pattern.compile("[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}", Pattern.CASE_INSENSITIVE);
    private static final Pattern SQL_INJECTION = Pattern.compile(
            "(?i)(drop\\s+table|union\\s+select|select\\s+\\*\\s+from\\s+information_schema|;\\s*delete\\s+from)");
    private static final Pattern PROMPT_INJECTION = Pattern.compile(
            "(?i)(ignore\\s+(all\\s+)?(previous|above|earlier)\\s+instructions|disregard\\s+(the\\s+)?system|jailbreak|reveal\\s+your\\s+system\\s+prompt)");

    private QuadGuardrails() {
    }

    /** Resolves one guardrail name (case-insensitive); {@code null} for unknown names. */
    public static Guardrail fromName(String name) {
        if (name == null || name.isBlank()) {
            return null;
        }
        return switch (name.strip().toLowerCase(Locale.ROOT)) {
            case "pii", "pii-filter", "email", "emails" -> (messages, context) ->
                    EMAIL.matcher(String.valueOf(messages)).find()
                            ? GuardrailResult.block("PII detected (email address)")
                            : GuardrailResult.ok();
            case "sql-injection", "sqli" -> (messages, context) ->
                    SQL_INJECTION.matcher(String.valueOf(messages)).find()
                            ? GuardrailResult.block("Potential SQL injection detected")
                            : GuardrailResult.ok();
            case "prompt-injection", "jailbreak" -> (messages, context) ->
                    PROMPT_INJECTION.matcher(String.valueOf(messages)).find()
                            ? GuardrailResult.block("Prompt injection attempt detected")
                            : GuardrailResult.ok();
            default -> {
                log.warn("Unknown guardrail '{}' — ignoring", name);
                yield null;
            }
        };
    }

    /** Resolves guardrail names to implementations; unknown names are skipped with a warning. */
    public static List<Guardrail> fromNames(List<String> names) {
        List<Guardrail> out = new ArrayList<>();
        if (names == null) {
            return out;
        }
        for (String name : names) {
            Guardrail g = fromName(name);
            if (g != null) {
                out.add(g);
            }
        }
        return out;
    }

    /**
     * Builds the PII anonymizer hook. Unknown/empty mask names fall back to
     * {@code EMAIL, PHONE_NUMBER, CREDIT_CARD}; masking replaces matches with {@code [REDACTED]}.
     */
    public static PiiAnonymizerHook piiAnonymizer(List<String> maskTypes) {
        Set<PiiAnonymizerHook.MaskType> masks = new LinkedHashSet<>();
        if (maskTypes != null) {
            for (String raw : maskTypes) {
                if (raw == null || raw.isBlank()) {
                    continue;
                }
                try {
                    masks.add(PiiAnonymizerHook.MaskType.valueOf(raw.strip().toUpperCase(Locale.ROOT)));
                } catch (IllegalArgumentException e) {
                    log.warn("Unknown PII mask type '{}' — ignoring", raw);
                }
            }
        }
        if (masks.isEmpty()) {
            masks.addAll(DEFAULT_MASKS);
        }
        return new PiiAnonymizerHook(masks, PiiAnonymizerHook.BlockAction.REDACT, "[REDACTED]");
    }

    /** Audit-trail hook (in-memory store; replace with a persistent one in production). */
    public static AuditTrailHook auditHook() {
        return new AuditTrailHook(new QuadCapabilities.InMemoryAuditStore());
    }
}
