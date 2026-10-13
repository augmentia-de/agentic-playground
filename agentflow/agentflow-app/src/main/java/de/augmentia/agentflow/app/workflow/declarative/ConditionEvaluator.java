package de.augmentia.agentflow.app.workflow.declarative;

/**
 * Minimal, deterministic condition language used by {@code decision} branches and {@code loop}
 * exit conditions. Supported forms (case-insensitive substring matching unless noted):
 *
 * <ul>
 *   <li>(blank / null) → always true (default branch)</li>
 *   <li>{@code contains:<text>} → context contains text</li>
 *   <li>{@code notcontains:<text>} → context does not contain text</li>
 *   <li>{@code starts:<text>} → context starts with text (trimmed)</li>
 *   <li>{@code equals:<text>} → trimmed context equals text</li>
 *   <li>{@code truthy} → context contains "true"</li>
 *   <li>{@code falsy} → context contains "false" or is blank</li>
 * </ul>
 */
public final class ConditionEvaluator {

    private ConditionEvaluator() {
    }

    public static boolean matches(String condition, String context) {
        String ctx = context == null ? "" : context;
        if (condition == null || condition.isBlank()) {
            return true;
        }
        String c = condition.trim();
        int colon = c.indexOf(':');
        String op = (colon < 0 ? c : c.substring(0, colon)).trim().toLowerCase();
        String arg = colon < 0 ? "" : c.substring(colon + 1);
        return switch (op) {
            case "contains" -> ctx.toLowerCase().contains(arg.toLowerCase());
            case "notcontains" -> !ctx.toLowerCase().contains(arg.toLowerCase());
            case "starts" -> ctx.trim().toLowerCase().startsWith(arg.trim().toLowerCase());
            case "equals" -> ctx.trim().equalsIgnoreCase(arg.trim());
            case "truthy" -> ctx.toLowerCase().contains("true");
            case "falsy" -> ctx.isBlank() || ctx.toLowerCase().contains("false");
            default -> false;
        };
    }
}
