package de.augmentia.agentflow.app.config;

import org.springframework.ai.openai.http.okhttp.OpenAiHttpClientBuilderCustomizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * Header wiring for LLM calls — driven purely by configuration
 * ({@code agentflow.llm.headers.*}). If nothing is configured, <b>no headers</b> are set.
 *
 * <p>{@link LlmHeaderResolver} is the single source of resolved headers for every stack. The
 * Spring AI client gets them per request via an OkHttp interceptor; the LangChain4j path is fed by
 * {@link LlmConfigBridge}. There is no provider-specific special case.</p>
 */
@Configuration
@EnableConfigurationProperties(AgentFlowLlmHeaderProperties.class)
public class LlmHeaderConfig {

    /** The single source of resolved headers for all stacks (Spring AI, LangChain4j, HTTP). */
    @Bean
    public LlmHeaderResolver llmHeaderResolver(AgentFlowLlmHeaderProperties properties,
                                               @Value("${spring.ai.openai.api-key:}") String apiKey) {
        return () -> properties.resolve(RequestContext.current(), apiKey);
    }

    /** Adds the configured headers to every Spring AI (OpenAI) request; no-op when none are set. */
    @Bean
    public OpenAiHttpClientBuilderCustomizer llmHeadersCustomizer(LlmHeaderResolver resolver) {
        return builder -> builder.interceptor(chain -> {
            Map<String, String> headers = resolver.resolve();
            if (headers.isEmpty()) {
                return chain.proceed(chain.request());
            }
            var request = chain.request().newBuilder();
            headers.forEach(request::header);
            return chain.proceed(request.build());
        });
    }
}
