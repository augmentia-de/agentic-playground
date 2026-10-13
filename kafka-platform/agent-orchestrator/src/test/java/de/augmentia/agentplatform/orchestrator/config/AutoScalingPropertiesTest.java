package de.augmentia.agentplatform.orchestrator.config;

import de.augmentia.agentplatform.common.kafka.TopicNames;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The scaling signal is configurable because it currently is not what the name promises.
 *
 * <p>{@code AutoScalingService} scales <em>workers</em> but by default measures the lag of
 * {@code agent.control.loop}. No worker reads that topic — it is the control topic of the
 * orchestrator itself ({@code DagTopicListener}). Worker scaling from that signal would
 * yield a decision unrelated to the workload.
 *
 * <p>That is why topic and group are set as properties and not hard-coded: switching to a
 * real task topic is one line in the configuration. The
 * The default remains the previous value so that existing deployments keep their behaviour.
 */
@DisplayName("OrchestratorProperties — Auto-Scaling-Signal")
class AutoScalingPropertiesTest {

    private static OrchestratorProperties.AutoScaling shippedDefaults() throws Exception {
        var loaded = new YamlPropertySourceLoader()
                .load("application", new ClassPathResource("application.yml"));
        var sources = new MutablePropertySources();
        loaded.forEach(sources::addLast);
        return new Binder(ConfigurationPropertySources.from(sources))
                .bind("app.autoscaling", OrchestratorProperties.AutoScaling.class)
                .get();
    }

    @Test
    @DisplayName("the default points at the topic that was measured before")
    void defaultTopicUnchanged() {
        var cfg = new OrchestratorProperties.AutoScaling(true, 30, 500, 2, 20, null, null);

        assertThat(cfg.lagTopic()).isEqualTo(TopicNames.LOOP);
        assertThat(cfg.lagGroupId()).isEqualTo(OrchestratorProperties.AutoScaling.DEFAULT_LAG_GROUP_ID);
    }

    @Test
    @DisplayName("empty and null fall back to the default")
    void blankFallsBackToDefault() {
        var cfg = new OrchestratorProperties.AutoScaling(true, 30, 500, 2, 20, "  ", "");

        assertThat(cfg.lagTopic()).isEqualTo(TopicNames.LOOP);
        assertThat(cfg.lagGroupId()).isEqualTo(OrchestratorProperties.AutoScaling.DEFAULT_LAG_GROUP_ID);
    }

    @Test
    @DisplayName("topic and group can be switched without a code change")
    void signalIsOverridable() {
        var cfg = new OrchestratorProperties.AutoScaling(
                true, 30, 500, 2, 20, "agent.tasks.planner", "quad-workflow-worker");

        assertThat(cfg.lagTopic()).isEqualTo("agent.tasks.planner");
        assertThat(cfg.lagGroupId()).isEqualTo("quad-workflow-worker");
    }

    @Test
    @DisplayName("the shipped application.yml carries both values")
    void shippedApplicationYamlDeclaresSignal() throws Exception {
        var cfg = shippedDefaults();

        assertThat(cfg.lagTopic())
                .as("the default remains the topic that was measured before")
                .isEqualTo(TopicNames.LOOP);
        assertThat(cfg.lagGroupId())
                .as("the group must match the topic — here: the orchestrator itself")
                .isEqualTo(OrchestratorProperties.AutoScaling.DEFAULT_LAG_GROUP_ID);
    }

    @Test
    @DisplayName("die YAML-Schwellen bleiben unveraendert")
    void shippedThresholdsUnchanged() throws Exception {
        var cfg = shippedDefaults();

        assertThat(cfg.enabled()).isTrue();
        assertThat(cfg.pollIntervalSeconds()).isEqualTo(30);
        assertThat(cfg.lagThreshold()).isEqualTo(500);
        assertThat(cfg.minWorkers()).isEqualTo(2);
        assertThat(cfg.maxWorkers()).isEqualTo(20);
    }
}