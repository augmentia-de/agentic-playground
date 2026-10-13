package de.augmentia.agentflow.app.memory;

import de.augmentia.quad.core.session.memory.MemoryEntry;
import de.augmentia.quad.core.session.memory.PersistentMemoryStore;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@link PersistentMemoryStore} for the test environment. Swap for the
 * JDBC-backed store in production (same interface).
 */
@Component
public class InMemoryMemoryStore implements PersistentMemoryStore {

    private final Map<String, MemoryEntry> byId = new ConcurrentHashMap<>();

    @Override
    public void remember(MemoryEntry item) {
        byId.put(item.getId(), item);
    }

    @Override
    public List<MemoryEntry> findByScope(String scope) {
        return byId.values().stream()
                .filter(e -> scope != null && scope.equals(e.getScope()))
                .sorted(Comparator.comparing(MemoryEntry::getCreatedAt).reversed())
                .toList();
    }

    @Override
    public List<MemoryEntry> listAll() {
        return List.copyOf(byId.values());
    }

    @Override
    public void updateContent(String id, String content) {
        MemoryEntry e = byId.get(id);
        if (e != null) e.setContent(content);
    }

    @Override
    public void deleteAll() {
        byId.clear();
    }

    @Override
    public void forget(String id) {
        byId.remove(id);
    }
}
