package de.augmentia.agentflow.app.workflow.declarative;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * File-backed persistence for workflow/agent authoring: writes published definitions to
 * {@code agentflow.workflows.state-dir} and keeps a previous-version snapshot before each
 * overwrite. The state directory can be version-controlled with git; disabling is done by an
 * empty {@code state-dir}.
 */
@Component
public class WorkflowDefinitionStore {

    private static final Logger log = LoggerFactory.getLogger(WorkflowDefinitionStore.class);

    private final ObjectMapper mapper;
    private final Path stateDir;
    private final Path versionsDir;

    public WorkflowDefinitionStore(ObjectMapper mapper,
                                   @Value("${agentflow.workflows.state-dir:target/agentflow-workflow-overrides}") String dir) {
        this.mapper = mapper;
        this.stateDir = (dir == null || dir.isBlank()) ? null : Path.of(dir);
        this.versionsDir = stateDir == null ? null : stateDir.resolve("versions");
    }

    public boolean enabled() {
        return stateDir != null;
    }

    public Path stateDir() {
        return stateDir;
    }

    /** Loads all persisted overrides (best effort; invalid files are skipped). */
    public List<JsonNode> loadOverrides() {
        if (!enabled() || !Files.isDirectory(stateDir)) {
            return List.of();
        }
        List<JsonNode> out = new ArrayList<>();
        try (Stream<Path> stream = Files.list(stateDir)) {
            stream.filter(p -> Files.isRegularFile(p) && p.getFileName().toString().endsWith(".json"))
                    .sorted()
                    .forEach(p -> {
                        try {
                            out.add(mapper.readTree(Files.readString(p)));
                        } catch (Exception e) {
                            log.warn("Skipping unreadable override {}: {}", p, e.getMessage());
                        }
                    });
        } catch (IOException e) {
            log.warn("Cannot read workflow state dir {}", stateDir, e);
        }
        return out;
    }

    /** Persists a published definition and snapshots the previous content as a version. */
    public void persist(JsonNode published) {
        if (!enabled()) {
            return;
        }
        try {
            Files.createDirectories(stateDir);
            String id = published.path("mainWorkflow").path("id").asText();
            Path target = stateDir.resolve(id + ".json");
            if (Files.exists(target)) {
                Path vDir = versionsDir.resolve(id);
                Files.createDirectories(vDir);
                String stamp = Instant.now().toString().replace(':', '-');
                Files.copy(target, vDir.resolve(stamp + ".json"));
            }
            Files.writeString(target, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(published));
            log.info("Persisted workflow definition '{}' to {}", id, target);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to persist workflow definition", e);
        }
    }

    public List<String> versions(String id) {
        if (!enabled()) {
            return List.of();
        }
        Path vDir = versionsDir.resolve(id);
        if (!Files.isDirectory(vDir)) {
            return List.of();
        }
        try (Stream<Path> stream = Files.list(vDir)) {
            return stream.map(p -> p.getFileName().toString()).sorted().toList();
        } catch (IOException e) {
            return List.of();
        }
    }

    public Optional<JsonNode> version(String id, String name) {
        if (!enabled() || name == null || name.contains("..") || name.contains("/")) {
            return Optional.empty();
        }
        Path p = versionsDir.resolve(id).resolve(name);
        if (!Files.isRegularFile(p)) {
            return Optional.empty();
        }
        try {
            return Optional.of(mapper.readTree(Files.readString(p)));
        } catch (IOException e) {
            return Optional.empty();
        }
    }
}
