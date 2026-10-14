package de.augmentia.quad.worker.config;

import de.augmentia.quad.worker.lifecycle.GracefulShutdown;
import de.augmentia.quad.worker.metrics.WorkerMetrics;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WP03/B38 — the worker used to declare {@code WorkerMetrics} and {@code GracefulShutdown} both
 * as {@code @Component} and as {@code @Bean}, which only booted because bean-definition-overriding
 * was enabled. Guard against that regression without starting a broker.
 */
class WorkerBeanUniquenessTest {

    @Test
    void metricsAndShutdownAreNotAlsoDeclaredAsBeans() {
        var beanReturnTypes = Arrays.stream(QuadWorkerConfig.class.getDeclaredMethods())
            .filter(m -> m.isAnnotationPresent(Bean.class))
            .map(Method::getReturnType)
            .toList();

        assertThat(beanReturnTypes)
            .as("duplicate @Bean definitions for the @Component types must be gone")
            .doesNotContain(WorkerMetrics.class, GracefulShutdown.class);
    }

    @Test
    void metricsAndShutdownAreStillComponentScanned() {
        assertThat(WorkerMetrics.class.isAnnotationPresent(Component.class))
            .as("WorkerMetrics must remain discoverable").isTrue();
        assertThat(GracefulShutdown.class.isAnnotationPresent(Component.class))
            .as("GracefulShutdown must remain discoverable").isTrue();
    }

    @Test
    void shippedConfigDoesNotEnableBeanOverriding() throws Exception {
        var loaded = new YamlPropertySourceLoader()
            .load("application", new ClassPathResource("application.yml"));
        var sources = new MutablePropertySources();
        loaded.forEach(sources::addLast);

        var overriding = new Binder(ConfigurationPropertySources.from(sources))
            .bind("spring.main.allow-bean-definition-overriding", Boolean.class)
            .orElse(false);

        assertThat(overriding)
            .as("overriding masks duplicate-bean bugs and must stay disabled")
            .isFalse();
    }
}
