package de.augmentia.quad.worker.config;

import dev.langchain4j.model.chat.request.ChatRequest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for ChatModelRegistry without a Spring context.
 */
class ChatModelUnitTest {

    @Test
    void mockModelIsRegistered() {
        var registry = new ChatModelRegistry();
        registry.register("mock", new ChatModelRegistry.MockChatModel());
        
        assertThat(registry.keys()).containsExactlyInAnyOrder("mock");
        
        var model = registry.get("mock");
        assertThat(model).isInstanceOf(ChatModelRegistry.MockChatModel.class);
    }

    @Test
    void unknownKeyThrowsException() {
        var registry = new ChatModelRegistry();
        
        assertThatThrownBy(() -> registry.get("nonexistent"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown model key");
    }

    @Test
    void duplicateRegistrationThrows() {
        var registry = new ChatModelRegistry();
        registry.register("test", new ChatModelRegistry.MockChatModel());
        
        assertThatThrownBy(() -> registry.register("test", new ChatModelRegistry.MockChatModel()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("already registered");
    }

    @Test
    void mockChatResponseContainsJson() {
        var mock = new ChatModelRegistry.MockChatModel();
        // Explicitly call the ChatRequest overload to avoid ambiguity
        ChatRequest nullRequest = null;
        var response = mock.chat(nullRequest);
        
        String content = response.aiMessage().text();
        assertThat(content).contains("Mock response");
        assertThat(content).contains("confidence");
        assertThat(content).contains("COMPLETED");
    }
}