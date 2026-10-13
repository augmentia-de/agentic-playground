package de.augmentia.agentplatform.orchestrator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guard for the component scan.
 *
 * <p>The orchestrator never booted: {@code IntakeListener} hard-requires the
 * {@code EmailInitiatorStore} in its constructor, which is declared as {@code @Service} in
 * {@code de.augmentia.agentplatform.bridge.redis} and lies — outside the
 * package {@code de.augmentia.agentplatform.orchestrator} scanned by
 * {@code @SpringBootApplication}. The result was a startup abort, not a silent malfunction.
 *
 * <p>The same bug occurred when renaming {@code com.agentplatform} to
 * {@code de.augmentia.agentplatform}: {@code scanBasePackages}
 * still pointed at the old package, causing all 37 components of the module to
 * stay unregistered. The test back then only failed at exactly one point and
 * would have slipped through the other two assertions, because a scan over an
 * empty package is naturally empty.
 *
 * <p>No test had noticed because the entire orchestrator suite does not boot a
 * Spring context. This test needs none: it compares the
 * actual components on the classpath with the declared scan area
 * and raises the alarm as soon as a component lies outside.
 *
 * <p>It is therefore not a test of a single bean but of an <em>invariant</em>:
 * every {@code @Component}/{@code @Service} class of the module must lie within the
 * scan area.
 */
@DisplayName("ComponentScan deckt alle Komponenten ab")
class ComponentScanCoverageTest {

    /** Base in which all components of the module actually lie. */
    private static final String MODULE_ROOT = "de.augmentia.agentplatform";

    @Test
    @DisplayName("Every @Component class lies in the declared scanBasePackages")
    void everyComponentIsInsideTheScanBase() {
        Set<String> scanBases = declaredScanBases();

        assertThat(scanBases)
                .as("OrchestratorApplication must set scanBasePackages explicitly — otherwise "
                    + "all components under de.augmentia.agentplatform.bridge are ignored")
                .containsExactly(MODULE_ROOT);

        Set<String> outside = discoveredComponents().stream()
                .filter(className -> !isInside(className, scanBases))
                .collect(Collectors.toCollection(TreeSet::new));

        assertThat(outside)
                .as("These components would never be registered as beans")
                .isEmpty();
    }

    @Test
    @DisplayName("The unconditional bridge components are in the scan and are expected as beans")
    void unconditionalBridgeComponentsAreDiscoverable() {
        Set<String> components = discoveredComponents();

        // these three carry NO @ConditionalOnProperty and are therefore always
        // active. That is exactly why the startup broke at EmailInitiatorStore: the
        // IntakeListener requires it hard, and it lay outside the scan area.
        assertThat(components).contains(
                "de.augmentia.agentplatform.bridge.redis.EmailInitiatorStore",
                "de.augmentia.agentplatform.bridge.email.KafkaProducer",
                "de.augmentia.agentplatform.bridge.web.UnifiedApiGateway");
    }

    @Test
    @DisplayName("The conditional bridge components are greyed out by default")
    void conditionalBridgeComponentsAreOptIn() {
        // these four carry @ConditionalOnProperty and are therefore inactive by
        // default. Intended are:
        //   app.email.transformer.enabled, app.email.responder.enabled,
        //   app.email.imap.enabled, app.vault.enabled
        //
        // Important: here the ANNOTATION is checked, not the result of a
        // component scan. A ClassPathScanningCandidateComponentProvider
        // evaluates @ConditionalOnProperty — it finds conditional classes
        // always. A scan-based test can therefore not prove "inactive by
        // default" at all; it previously only saw an empty set, because it scanned over the
        // orphaned package com.agentplatform.
        //
        // Checked instead: the expected property is annotated and
        // matchIfMissing is not set (= default off).
        record OptIn(String className, String property) {}
        List<OptIn> optIns = List.of(
                new OptIn("de.augmentia.agentplatform.bridge.email.EmailInputTransformer",
                        "app.email.transformer.enabled"),
                new OptIn("de.augmentia.agentplatform.bridge.email.MailResponseHandler",
                        "app.email.responder.enabled"),
                new OptIn("de.augmentia.agentplatform.bridge.email.ImapMailListener",
                        "app.email.imap.enabled"),
                new OptIn("de.augmentia.agentplatform.bridge.vault.SecretResolver",
                        "app.vault.enabled"));

        for (OptIn optIn : optIns) {
            ConditionalOnProperty condition = load(optIn.className()).getAnnotation(ConditionalOnProperty.class);
            assertThat(condition)
                    .as("%s must be guarded via @ConditionalOnProperty", optIn.className())
                    .isNotNull();
            assertThat(List.of(condition.name()))
                    .as("%s switches via %s", optIn.className(), optIn.property())
                    .contains(optIn.property());
            assertThat(condition.havingValue())
                    .as("%s is only active with havingValue=true", optIn.className())
                    .isEqualTo("true");
            assertThat(condition.matchIfMissing())
                    .as("%s must not have matchIfMissing=true, otherwise it runs by default",
                            optIn.className())
                    .isFalse();
        }
    }

    private Class<?> load(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new AssertionError("Bridge component missing: " + className, e);
        }
    }

    @Test
    @DisplayName("agent-common deliberately contains no components")
    void agentCommonStaysComponentFree() {
        assertThat(discoveredComponents().stream()
                .filter(cn -> cn.startsWith("de.augmentia.agentplatform.common")))
                .as("A broader scan must not pull unexpected beans from agent-common")
                .isEmpty();
    }

    // ── Hilfen ──

    /** Reads the actually declared scan base from the annotation. */
    private Set<String> declaredScanBases() {
        SpringBootApplication annotation =
                OrchestratorApplication.class.getAnnotation(SpringBootApplication.class);
        assertThat(annotation)
                .as("OrchestratorApplication carries @SpringBootApplication")
                .isNotNull();
        return Set.of(annotation.scanBasePackages());
    }

    private Set<String> discoveredComponents() {
        var provider = new ClassPathScanningCandidateComponentProvider(false);
        provider.addIncludeFilter(new AnnotationTypeFilter(Component.class));
        return provider.findCandidateComponents(MODULE_ROOT).stream()
                .map(org.springframework.beans.factory.config.BeanDefinition::getBeanClassName)
                .filter(java.util.Objects::nonNull)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private static boolean isInside(String className, Set<String> bases) {
        return bases.stream().anyMatch(base -> className.startsWith(base + "."));
    }
}
