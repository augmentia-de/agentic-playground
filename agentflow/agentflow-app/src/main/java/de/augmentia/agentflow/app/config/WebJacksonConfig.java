package de.augmentia.agentflow.app.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;

/**
 * Uses Jackson <b>2</b> ({@code com.fasterxml.jackson}) for REST JSON.
 *
 * <p>Spring Boot 4 auto-configures only Jackson 3 ({@code tools.jackson}) for HTTP.
 * AgentFlow (and the reused quad-core / Temporal / workflow store) is built on Jackson 2, so
 * controllers that return or receive {@code com.fasterxml.jackson.databind.JsonNode} would
 * otherwise break: the node is serialized as its bean properties (e.g. {@code array},
 * {@code bigDecimal}, …) and request binding fails with HTTP 500.</p>
 *
 * <p>The converter is registered <b>after</b> {@link StringHttpMessageConverter}: SSE data frames
 * (and other String payloads) carry a {@code null} media type, and a JSON converter placed before
 * the String converter would JSON-encode those plain Strings (double-encoding — e.g. the run list
 * must then be parsed twice). It still precedes the Jackson 3 converter, so JsonNode request and
 * response bodies keep using Jackson 2.</p>
 */
@Configuration
public class WebJacksonConfig implements WebMvcConfigurer {

    private final ObjectMapper jackson2;

    public WebJacksonConfig(ObjectMapper jackson2) {
        this.jackson2 = jackson2;
    }

    @Override
    public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
        converters.removeIf(MappingJackson2HttpMessageConverter.class::isInstance);
        // Insert after the last String converter (keeps plain Strings raw), but still before any
        // later JSON (Jackson 3) converter so JsonNode bodies use Jackson 2.
        int insertAt = 0;
        for (int i = 0; i < converters.size(); i++) {
            if (converters.get(i) instanceof StringHttpMessageConverter) {
                insertAt = i + 1;
            }
        }
        converters.add(insertAt, new MappingJackson2HttpMessageConverter(jackson2));
    }
}
