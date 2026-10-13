package de.augmentia.agentflow.app.memory;

import de.augmentia.quad.core.session.memory.MemoryCategory;
import de.augmentia.quad.core.session.memory.MemoryEntry;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryMemoryStoreTest {

    private MemoryEntry entry(String id, String scope, String content) {
        return new MemoryEntry(id, scope, content, null, MemoryCategory.PROJECT_FACT, Instant.now(), false);
    }

    @Test
    void remembersAndRecallsByScope() {
        InMemoryMemoryStore store = new InMemoryMemoryStore();
        store.remember(entry("1", "project-a", "the sky is blue"));
        store.remember(entry("2", "project-b", "other fact"));

        assertThat(store.findByScope("project-a")).hasSize(1);
        assertThat(store.findByScope("project-b")).hasSize(1);
        assertThat(store.listAll()).hasSize(2);

        String injection = store.formatForInjection("project-a");
        assertThat(injection).contains("PROJECT_FACT").contains("the sky is blue");
    }

    @Test
    void forgetRemovesEntry() {
        InMemoryMemoryStore store = new InMemoryMemoryStore();
        store.remember(entry("1", "s", "x"));
        store.forget("1");
        assertThat(store.findByScope("s")).isEmpty();
    }

    @Test
    void updateContentKeepsEntry() {
        InMemoryMemoryStore store = new InMemoryMemoryStore();
        store.remember(entry("1", "s", "old"));
        store.updateContent("1", "new");
        assertThat(store.formatForInjection("s")).contains("new");
    }
}
