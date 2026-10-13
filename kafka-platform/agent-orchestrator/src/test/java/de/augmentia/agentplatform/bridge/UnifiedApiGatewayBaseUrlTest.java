package de.augmentia.agentplatform.bridge;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The gateway URL default follows the port of the quad application.
 *
 * <p>Der Befund: {@code @Value("${quarkus.base-url:http://localhost:8086}")} war an
 * set anywhere, while the quad port is configurable via {@code quarkus.http.port}
 * A set {@code QUARKUS_HTTP_PORT=9090} thereby yielded
 * still einen 502.
 *
 * <p>The test checks the resolution of the nested placeholder — not the
 * gateway itself. Exactly that resolution is the risk: {@code ${a:${b:8086}}} has to
 * work as a nested default, and it has to follow the property that
 * actually sets the port.
 */
class UnifiedApiGatewayBaseUrlTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(ProbeConfig.class);

    @Configuration
    static class ProbeConfig {
        @Bean
        UrlProbe probe(@Value("${quarkus.base-url:http://localhost:${quarkus.http.port:8086}}") String url) {
            return new UrlProbe(url);
        }
    }

    record UrlProbe(String url) {}

    @Test
    void ohne_konfiguration_zeigt_der_default_auf_8086() {
        runner.run(ctx -> assertThat(ctx.getBean(UrlProbe.class).url())
                .isEqualTo("http://localhost:8086"));
    }

    @Test
    void ein_gesetzter_port_folgt_in_den_default_ein() {
        runner.withPropertyValues("quarkus.http.port=9090")
                .run(ctx -> assertThat(ctx.getBean(UrlProbe.class).url())
                        .as("the default must follow the property that actually controls the port")
                        .isEqualTo("http://localhost:9090"));
    }

    @Test
    void eine_ausdruecklich_gesetzte_url_hat_vorrang() {
        runner.withPropertyValues(
                        "quarkus.http.port=9090",
                        "quarkus.base-url=http://quad.internal:7070")
                .run(ctx -> assertThat(ctx.getBean(UrlProbe.class).url())
                        .as("whoever sets a URL explicitly wants it")
                        .isEqualTo("http://quad.internal:7070"));
    }
}