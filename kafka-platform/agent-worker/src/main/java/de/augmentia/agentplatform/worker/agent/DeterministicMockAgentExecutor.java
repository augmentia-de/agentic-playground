package de.augmentia.agentplatform.worker.agent;

import de.augmentia.agentplatform.common.agent.AgentExecutionResult;
import de.augmentia.agentplatform.common.agent.AgentExecutor;
import de.augmentia.agentplatform.common.event.AgentTaskEvent;
import de.augmentia.agentplatform.common.model.ResultStatus;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

@Component
public class DeterministicMockAgentExecutor implements AgentExecutor {

    @Override
    public String agentType() {
        return "*";
    }

    @Override
    public AgentExecutionResult execute(AgentTaskEvent task) {
        Map<String, Object> parameters = task.payload() == null || task.payload().parameters() == null
                ? Map.of()
                : task.payload().parameters();
        Map<String, Object> input = asMap(parameters.get("input"));
        Map<String, Object> context = asMap(parameters.get("context"));

        String stepId = string(parameters.get("stepId"), task.header().taskId());
        int iteration = integer(parameters.get("iteration"), 1);
        int attempt = task.header() == null ? 1 : task.header().attempt();
        String agentType = task.agentConfig() == null ? "mock" : task.agentConfig().agentType();

        String seed = String.join("|",
                nullSafe(task.header() == null ? null : task.header().workflowId()),
                nullSafe(task.header() == null ? null : task.header().taskId()),
                nullSafe(agentType),
                stepId,
                Integer.toString(iteration),
                Integer.toString(attempt),
                nullSafe(task.payload() == null ? null : task.payload().inputPrompt()));
        long hash = sha256Long(seed);

        Map<String, Object> resultData = new LinkedHashMap<>();
        resultData.put("agentType", agentType);
        resultData.put("stepId", stepId);
        resultData.put("iteration", iteration);
        resultData.put("attempt", attempt);
        resultData.put("executor", "deterministic-mock");
        resultData.put("summary", "Deterministic mock output for " + agentType + "/" + stepId);
        resultData.put("checksum", Long.toHexString(hash));
        resultData.put("score", normalized(hash));

        copyIfPresent(input, resultData, "mode");
        copyIfPresent(input, resultData, "runLint");
        copyIfPresent(input, resultData, "qualityGrade");

        Double confidence = confidence(agentType, stepId, iteration, hash, input, context);
        if (confidence != null) {
            resultData.put("confidence", confidence);
        }

        Map<String, Object> overrides = stepMap(input, "resultDataOverrides", stepId);
        resultData.putAll(overrides);

        String forcedStatus = string(stepValue(input, "forceStatus", stepId), null);
        Integer failAttempts = integerOrNull(stepValue(input, "failAttempts", stepId));
        String errorMessage = string(stepValue(input, "errorMessage", stepId), null);

        if (forcedStatus != null) {
            ResultStatus status = ResultStatus.valueOf(forcedStatus.toUpperCase(Locale.ROOT));
            return switch (status) {
                case COMPLETED -> AgentExecutionResult.completed(resultData);
                case FAILED_RETRYABLE -> AgentExecutionResult.failedRetryable(errorMessage == null
                        ? "Deterministic retryable failure for " + stepId
                        : errorMessage);
                case FAILED_FATAL -> AgentExecutionResult.failedFatal(errorMessage == null
                        ? "Deterministic fatal failure for " + stepId
                        : errorMessage);
                case NEEDS_HITL -> AgentExecutionResult.needsHitl(errorMessage == null
                        ? "Deterministic human review required for " + stepId
                        : errorMessage, resultData);
            };
        }

        if (failAttempts != null && attempt <= failAttempts) {
            return AgentExecutionResult.failedRetryable(errorMessage == null
                    ? "Deterministic retryable failure for attempt " + attempt + " of " + stepId
                    : errorMessage);
        }

        return AgentExecutionResult.completed(resultData);
    }

    private Double confidence(String agentType, String stepId, int iteration, long hash,
                              Map<String, Object> input, Map<String, Object> context) {
        String normalizedAgentType = agentType == null ? "" : agentType.toLowerCase(Locale.ROOT);
        if (!normalizedAgentType.contains("research")
                && !normalizedAgentType.contains("refin")
                && !normalizedAgentType.contains("quality")
                && !normalizedAgentType.contains("review")) {
            return null;
        }

        Double explicit = doubleOrNull(input.get("researchConfidence"));
        Double base = explicit;
        if (base == null) {
            base = doubleOrNull(input.get("refinementBaseConfidence"));
        }
        if (base == null && normalizedAgentType.contains("refin")) {
            Map<String, Object> research = asMap(context.get("research_step"));
            Map<String, Object> researchResult = asMap(research.get("resultData"));
            base = doubleOrNull(researchResult.get("confidence"));
        }
        if (base == null) {
            base = normalized(hash);
        }

        if (normalizedAgentType.contains("refin")) {
            double improved = base + 0.12d * (iteration - 1);
            return Math.min(0.95d, Math.round(improved * 100.0d) / 100.0d);
        }
        return base;
    }

    private static Object stepValue(Map<String, Object> input, String key, String stepId) {
        Map<String, Object> values = asMap(input.get(key));
        return values.get(stepId);
    }

    private static Map<String, Object> stepMap(Map<String, Object> input, String key, String stepId) {
        Object value = stepValue(input, key, stepId);
        return value == null ? Map.of() : asMap(value);
    }

    private static void copyIfPresent(Map<String, Object> source, Map<String, Object> target, String key) {
        if (source.containsKey(key)) {
            target.put(key, source.get(key));
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value) {
        if (value instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        return Map.of();
    }

    private static String string(Object value, String fallback) {
        return value == null ? fallback : String.valueOf(value);
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }

    private static int integer(Object value, int fallback) {
        Integer parsed = integerOrNull(value);
        return parsed == null ? fallback : parsed;
    }

    private static Integer integerOrNull(Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String text && !text.isBlank()) {
            try {
                return Integer.parseInt(text);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private static Double doubleOrNull(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof String text && !text.isBlank()) {
            try {
                return Double.parseDouble(text);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private static double normalized(long hash) {
        return Math.floorMod(hash, 1000L) / 1000.0d;
    }

    private static long sha256Long(String material) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(material.getBytes(StandardCharsets.UTF_8));
            long value = 0L;
            for (int i = 0; i < 8; i++) {
                value = (value << 8) | (bytes[i] & 0xffL);
            }
            return value & Long.MAX_VALUE;
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is required for deterministic mock agents", ex);
        }
    }
}
