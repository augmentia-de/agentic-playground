package de.augmentia.quad.worker.config;

import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.openai.OpenAiChatModel;
import dev.langchain4j.model.output.TokenUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Key-based registry for LLM chat models.
 *
 * Phase 1: mock model (fallback if there are no API keys)
 * Phase 2+: real OpenAI/OpenRouter models from .env / environment
 */
public class ChatModelRegistry {

    private final Map<String, ChatModel> models = new ConcurrentHashMap<>();

    /**
     * Registers a real chat model under a key.
     */
    public void register(String key, ChatModel model) {
        if (models.containsKey(key)) {
            throw new IllegalArgumentException("Model already registered: " + key);
        }
        models.put(key, model);
    }

    /**
     * Returns a model by key. Fail-fast if not found.
     */
    public ChatModel get(String key) {
        ChatModel model = models.get(key);
        if (model == null) {
            throw new IllegalArgumentException("Unknown model key: " + key + ". Available: " + models.keySet());
        }
        return model;
    }

    /**
     * Available model keys.
     */
    public java.util.Set<String> keys() {
        return models.keySet();
    }

    /**
     * Minimalist mock LLM that always generates structured outputs.
     */
    public static class MockChatModel implements ChatModel {

        @Override
        public ChatResponse chat(ChatRequest request) {
            String content = """
                {
                  "response": "Mock response from simple model",
                  "confidence": 0.95,
                  "status": "COMPLETED"
                }
                """;

            TokenUsage usage = new TokenUsage(10, 50, 60);
            
            return ChatResponse.builder()
                .aiMessage(dev.langchain4j.data.message.AiMessage.from(content))
                .tokenUsage(usage)
                .build();
        }
    }
}