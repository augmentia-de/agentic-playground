package de.augmentia.agentflow.app.config;

import de.augmentia.agentflow.core.AgentFlowState;
import org.bsc.langgraph4j.checkpoint.BaseCheckpointSaver;
import org.bsc.langgraph4j.checkpoint.SQLiteSaverV2;
import org.bsc.langgraph4j.serializer.std.ObjectStreamStateSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Persistent LangGraph4j checkpoint store (SQLite). Survives JVM/worker restarts, so a
 * graph interrupted for Human-in-the-Loop can be resumed later.
 *
 * <p>Swap {@link SQLiteSaverV2} for the Postgres/latest saver in production as needed.</p>
 */
@Configuration
public class CheckpointConfig {

    private static final Logger log = LoggerFactory.getLogger(CheckpointConfig.class);

    @Bean
    public BaseCheckpointSaver checkpointSaver(
            @Value("${agentflow.checkpoint.path:target/agentflow-checkpoints.db}") String path) {
        try {
            Path db = Path.of(path).toAbsolutePath().normalize();
            Path parent = db.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            log.info("Checkpoint store (SQLite): {}", db);
            return SQLiteSaverV2.builder()
                    .databasePath(db.toString())
                    .stateSerializer(new ObjectStreamStateSerializer<>(AgentFlowState::new))
                    .createTables(true)
                    .build();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create SQLite checkpoint saver at " + path, e);
        }
    }
}
