package de.augmentia.quad.core.gdpr;

import de.augmentia.quad.core.hook.pipeline.HookContexts;
import de.augmentia.quad.core.hook.pipeline.HookResult;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.UserMessage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** PII anonymization must mask case-insensitively and never mutate the running list. */
class PiiAnonymizerHookTest {

    private static HookContexts.BeforeModelCallContext ctx(List<ChatMessage> messages) {
        return new HookContexts.BeforeModelCallContext("s", new StringBuilder(), messages, List.of(), List.of());
    }

    private static PiiAnonymizerHook emailHook() {
        return new PiiAnonymizerHook(Set.of(PiiAnonymizerHook.MaskType.EMAIL),
                PiiAnonymizerHook.BlockAction.REDACT, "[PII]");
    }

    @SuppressWarnings("unchecked")
    private static List<ChatMessage> maskedMessages(HookResult result) {
        assertThat(result).isInstanceOf(HookResult.Modify.class);
        return (List<ChatMessage>) ((HookResult.Modify<?>) result).value();
    }

    @Test
    void doesNotMutateOriginalList() {
        var original = new ArrayList<ChatMessage>(
                List.of(UserMessage.from("mail me at user@example.com")));

        HookResult result = emailHook().beforeModelCall(ctx(original));

        // The running transcript keeps the original content ...
        assertThat(((UserMessage) original.get(0)).singleText()).contains("user@example.com");
        // ... while the masked copy travels in the hook result.
        var masked = maskedMessages(result);
        assertThat(((UserMessage) masked.get(0)).singleText())
                .doesNotContain("user@example.com")
                .contains("[PII]");
    }

    @Test
    void masksLowercaseEmail() {
        var original = new ArrayList<ChatMessage>(
                List.of(UserMessage.from("contact user@example.com please")));

        HookResult result = emailHook().beforeModelCall(ctx(original));

        assertThat(((UserMessage) maskedMessages(result).get(0)).singleText())
                .doesNotContain("user@example.com")
                .contains("[PII]");
    }

    @Test
    void leavesUntouchedMessagesAsContinue() {
        var original = new ArrayList<ChatMessage>(List.of(UserMessage.from("hello there")));

        HookResult result = emailHook().beforeModelCall(ctx(original));

        assertThat(result).isInstanceOf(HookResult.Continue.class);
    }

    @Test
    void throwActionCancelsWithoutMutating() {
        var hook = new PiiAnonymizerHook(Set.of(PiiAnonymizerHook.MaskType.EMAIL),
                PiiAnonymizerHook.BlockAction.THROW, "[PII]");
        var original = new ArrayList<ChatMessage>(List.of(UserMessage.from("user@example.com")));

        HookResult result = hook.beforeModelCall(ctx(original));

        assertThat(result).isInstanceOf(HookResult.Cancel.class);
        assertThat(((UserMessage) original.get(0)).singleText()).contains("user@example.com");
    }
}
