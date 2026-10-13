package de.augmentia.quad.worker.kafka;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.annotation.KafkaListener;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the wildcard pattern of the task listener really matches the topic names
 * that the orchestrator creates.
 *
 * <p>Background: in {@code application.yml} there was
 * {@code ${WORKER_TASK_TOPIC_PATTERN:agent\\\\.tasks\\\\..*}}. YAML treats
 * backslashes in plain scalars literally, so the double backslash ended up
 * literally in the regex. As a Java regex, {@code agent\\\\.tasks\\\\..*} however means
 * "agent, then a literal backslash" — it matched <em>no</em> topic. The
 * worker stayed at {@code partitions assigned: []} permanently and processed
 * nothing, without logging a single error.
 *
 * <p>The test reads the <em>real</em> {@code application.yml} from the classpath and
 * resolves the placeholder without an environment — thus with the default value that
 * also applies in operation as long as {@code WORKER_TASK_TOPIC_PATTERN} is not
 * set. In addition the default from the {@code @KafkaListener} annotation
 * is checked, because both places can diverge.
 */
@DisplayName("Task-Topic-Pattern matcht echte Topics")
class TaskTopicPatternTest {

    /** Die Topics, die {@code KafkaTopicInitializer} im Orchestrator anlegt. */
    private static final List<String> REAL_TOPICS = List.of(
            "agent.tasks.researcher",
            "agent.tasks.planner",
            "agent.tasks.coder",
            "agent.tasks.linter",
            "agent.tasks.quality",
            "agent.tasks.refiner",
            "agent.tasks.reviewer",
            "agent.tasks.fixer",
            "agent.tasks.tester",
            "agent.tasks.test_writer");

    /** Resolution of the placeholder without an environment — thus it uses the default. */
    private static String effectivePatternFromYaml() {
        try {
            var sources = new org.springframework.boot.env.YamlPropertySourceLoader()
                    .load("worker-test", new ClassPathResource("application.yml"));
            Map<String, Object> values = new java.util.LinkedHashMap<>();
            for (PropertySource<?> ps : sources) {
                if (ps instanceof org.springframework.core.env.EnumerablePropertySource<?> enumerable) {
                    for (String name : enumerable.getPropertyNames()) {
                        values.put(name, enumerable.getProperty(name));
                    }
                }
            }
            Object raw = values.get("worker.quad.task-topic-pattern");
            return resolvePlaceholder(raw == null ? null : raw.toString());
        } catch (Exception e) {
            throw new AssertionError("application.yml not readable from the test classpath", e);
        }
    }

    /**
     * Resolves a {@code ${name:default}} placeholder and returns the default.
     *
     * <p>Both sources — YAML and annotation — contain the same placeholder,
     * only with different names ({@code WORKER_TASK_TOPIC_PATTERN} resp.
     * {@code worker.quad.task-topic-pattern}). Without an environment set the
     * default applies, and that is exactly what is of interest here. Environment overrides are
     * deliberately not part of the test: they would be environment-dependent.
     */
    private static String resolvePlaceholder(String raw) {
        if (raw == null || !raw.contains("${")) {
            return raw;
        }
        int start = raw.indexOf("${") + 2;
        int end = raw.indexOf('}', start);
        if (end < 0) {
            return raw;
        }
        String body = raw.substring(start, end);
        int colon = body.indexOf(':');
        return colon < 0 ? body : body.substring(colon + 1);
    }

    @Test
    @DisplayName("application.yml: the pattern matches all real task topics")
    void yamlPatternMatchesAllRealTopics() {
        String pattern = effectivePatternFromYaml();
        assertThat(pattern).as("the property must be present in application.yml").isNotNull();

        assertThat(nonMatching(pattern))
                .as("Pattern aus application.yml ('%s') matcht keines der echten Task-Topics", pattern)
                .isEmpty();
    }

    @Test
    @DisplayName("application.yml: no double backslashes (the YAML trap)")
    void yamlPatternHasNoEscapedBackslash() {
        String pattern = effectivePatternFromYaml();
        assertThat(pattern)
                .as("Doppelter Backslash entsteht, wenn in YAML '\\\\\\\\.' statt '\\\\.' steht. "
                    + "The regex then searches for a literal backslash and matches nothing.")
                .doesNotContain("\\\\");
    }

    @Test
    @DisplayName("Der Default aus der @KafkaListener-Annotation matcht ebenfalls")
    void annotationDefaultMatchesAllRealTopics() {
        String pattern = resolvePlaceholder(taskPatternFromAnnotation());
        assertThat(pattern).as("topicPattern not findable in the annotation").isNotNull();

        assertThat(nonMatching(pattern))
                .as("the annotation default ('%s') matches no topic", pattern)
                .isEmpty();
    }

    @Test
    @DisplayName("The dot is really escaped — 'agentXtasksY' does not match")
    void escapedDotMatchesOnlyTheDot() {
        Pattern pattern = Pattern.compile(effectivePatternFromYaml());

        assertThat(pattern.matcher("agent.tasks.planner").matches()).isTrue();
        assertThat(pattern.matcher("agentXtasksYplanner").matches())
                .as("Without escaping any arbitrary character would match")
                .isFalse();
    }

    @Test
    @DisplayName("Control topics are not matched")
    void unrelatedTopicsDoNotMatch() {
        Pattern pattern = Pattern.compile(effectivePatternFromYaml());

        assertThat(pattern.matcher("agent.execution.results").matches()).isFalse();
        assertThat(pattern.matcher("agent.execution.dlq").matches()).isFalse();
        assertThat(pattern.matcher("__consumer_offsets").matches()).isFalse();
    }

    @Test
    @DisplayName("Beide Quellen liefern dasselbe Muster")
    void yamlAndAnnotationAgree() {
        assertThat(effectivePatternFromYaml())
                .as("YAML and annotation must define the same pattern, otherwise the "
                    + "worker hangs on a different pattern depending on the configuration source")
                .isEqualTo(resolvePlaceholder(taskPatternFromAnnotation()));
    }

    @Test
    @DisplayName("The pattern is a valid regex")
    void patternIsValid() {
        try {
            Pattern.compile(effectivePatternFromYaml());
        } catch (PatternSyntaxException e) {
            throw new AssertionError("the pattern is not a valid regex", e);
        }
    }

    // ── Hilfen ──

    private static List<String> nonMatching(String regex) {
        Pattern pattern = Pattern.compile(regex);
        return REAL_TOPICS.stream().filter(t -> !pattern.matcher(t).matches()).toList();
    }

    private String taskPatternFromAnnotation() {
        try {
            Method onTasks = WorkflowTaskListener.class.getMethod(
                    "onTasks", List.class,
                    org.springframework.kafka.support.Acknowledgment.class);
            return onTasks.getAnnotation(KafkaListener.class).topicPattern();
        } catch (NoSuchMethodException e) {
            throw new AssertionError(
                    "WorkflowTaskListener.onTasks not found — the signature changed, adapt the test.", e);
        }
    }
}
