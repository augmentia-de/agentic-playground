package de.augmentia.agentplatform.orchestrator.config;

import de.augmentia.agentplatform.bridge.email.MailResponseHandler;
import de.augmentia.agentplatform.orchestrator.kafka.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contract between listener signature and ack mode.
 *
 * <p>A listener that has an {@link Acknowledgment} parameter <em>and</em> a
 * factory with {@code AckMode.RECORD} throws at runtime on every
 * call: {@code IllegalStateException: No Acknowledgment available as an
 * argument …}. The record then fails in the error handling of the
 * container and is redelivered.
 *
 * <p>That is exactly what happened in operation: {@code IntakeListener.handleIntake} and
 * {@code ConfigUpdateConsumer.handleConfigUpdate} acknowledged manually but ran
 * on the standard factory in {@code RECORD} mode. The live call ended after
 * ten repetitions in the error handler — the intake message of a
 * started from the UI was therefore never processed regularly.
 *
 * <p>A unit test that calls the listener directly and mocks {@code Acknowledgment}
 * sees none of this: the mode only arises in the container. This test
 * checks the wiring of annotation and factory without booting a
 * Spring context.
 */
@DisplayName("Ack mode matches the listener signature")
class ListenerAckModeContractTest {

    /** All classes of the orchestrator that use @KafkaListener. */
    private static final List<Class<?>> LISTENER_CLASSES = List.of(
            IntakeListener.class,
            ConfigUpdateConsumer.class,
            HitlListener.class,
            DagTopicListener.class,
            SweepListener.class,
            MailResponseHandler.class);

    /** Context with both factories, but without TLS condition and without a real broker. */
    private static ContainerProperties.AckMode modeOf(String factoryBeanName) {
        try (var context = new org.springframework.context.annotation.AnnotationConfigApplicationContext()) {
            context.registerBean(org.springframework.kafka.core.ConsumerFactory.class,
                    () -> new org.springframework.kafka.core.DefaultKafkaConsumerFactory<>(
                            java.util.Map.of("group.id", "vertragstest")));
            context.register(KafkaAckConfig.class);
            context.refresh();

            var factory = context.getBean(factoryBeanName,
                    org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory.class);
            return factory.getContainerProperties().getAckMode();
        }
    }

    @Test
    @DisplayName("Jeder Listener mit Acknowledgment laeuft auf AckMode.MANUAL")
    void manualAckListenersUseManualAckMode() {
        List<String> violations = new ArrayList<>();

        for (Class<?> type : LISTENER_CLASSES) {
            for (Method method : type.getDeclaredMethods()) {
                KafkaListener listener = method.getAnnotation(KafkaListener.class);
                if (listener == null) {
                    continue;
                }
                boolean wantsAck = List.of(method.getParameterTypes()).contains(Acknowledgment.class);
                if (!wantsAck) {
                    continue;
                }
                ContainerProperties.AckMode mode = modeOf(listener.containerFactory());
                if (mode != ContainerProperties.AckMode.MANUAL) {
                    violations.add(type.getSimpleName() + "#" + method.getName()
                            + " requires an acknowledgment but runs on '"                            + listener.containerFactory() + "' mit AckMode." + mode);
                }
            }
        }

        assertThat(violations)
                .as("A listener with an acknowledgment parameter strictly needs AckMode.MANUAL, "
                    + "otherwise acknowledge() throws at runtime")
                .isEmpty();
    }

    @Test
    @DisplayName("The standard factory still acknowledges per record")
    void defaultFactoryStaysOnRecord() {
        // application.yml sets ack-mode: record. That value is not
        // recreated here but checked against the configuration.
        String declared = readAckModeFromApplicationYml();

        assertThat(declared)
                .as("The ack mode of the standard factory is in application.yml")
                .isEqualToIgnoringCase("record");
    }

    private static String readAckModeFromApplicationYml() {
        try (var in = ListenerAckModeContractTest.class.getResourceAsStream("/application.yml")) {
            if (in == null) {
                return "application.yml not on the test classpath";
            }
            String yaml = new String(in.readAllBytes());
            var matcher = java.util.regex.Pattern
                    .compile("ack-mode:\\s*(\\S+)").matcher(yaml);
            return matcher.find() ? matcher.group(1) : "not set";
        } catch (java.io.IOException e) {
            return "application.yml not readable";
        }
    }

    @Test
    @DisplayName("The manual factory exists independently of the TLS profile")
    void oneFactoryPerMode() {
        assertThat(modeOf("manualAckListenerContainerFactory"))
                .isEqualTo(ContainerProperties.AckMode.MANUAL);
    }

    @Test
    @DisplayName("The test coverage covers both manual listeners")
    void bothManualListenersAreCovered() {
        long withAck = LISTENER_CLASSES.stream()
                .flatMap(t -> java.util.Arrays.stream(t.getDeclaredMethods()))
                .filter(m -> m.getAnnotation(KafkaListener.class) != null)
                .filter(m -> List.of(m.getParameterTypes()).contains(Acknowledgment.class))
                .count();

        assertThat(withAck)
                .as("If this value drops, the contract test no longer checks anything")
                .isEqualTo(2);
    }
}
