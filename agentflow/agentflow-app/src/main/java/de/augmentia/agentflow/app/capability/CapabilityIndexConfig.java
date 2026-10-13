package de.augmentia.agentflow.app.capability;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.quad.core.capability.CapabilityIndex;
import de.augmentia.quad.core.capability.CapabilitySearch;
import de.augmentia.quad.core.capability.HnswCapabilityIndex;
import de.augmentia.quad.core.capability.PgVectorCapabilityIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

/**
 * Capability index + search wiring.
 *
 * <p>The index is backed by {@link PgVectorCapabilityIndex} when
 * {@code agentflow.capability.index=pgvector} and a {@link DataSource} bean is present;
 * otherwise it falls back to the dependency-free in-memory {@link HnswCapabilityIndex}.
 * This keeps local/test runs working without a database while allowing the production
 * deployment to use PostgreSQL.</p>
 */
@Configuration
public class CapabilityIndexConfig {

    private static final Logger log = LoggerFactory.getLogger(CapabilityIndexConfig.class);

    @Bean
    @ConditionalOnMissingBean
    public CapabilityIndex capabilityIndex(ObjectProvider<DataSource> dataSourceProvider,
                                           ObjectMapper objectMapper,
                                           @Value("${agentflow.capability.index:hnsw}") String mode,
                                           @Value("${agentflow.capability.table:capability_index}") String table) {
        DataSource dataSource = dataSourceProvider.getIfAvailable();
        if ("pgvector".equalsIgnoreCase(mode) && dataSource != null) {
            ensureTable(dataSource, table);
            log.info("CapabilityIndex: PgVector (table={})", table);
            return new PgVectorCapabilityIndex(dataSource, objectMapper, table);
        }
        log.info("CapabilityIndex: in-memory HNSW (mode={})", mode);
        return new HnswCapabilityIndex();
    }

    @Bean
    @ConditionalOnMissingBean
    public CapabilitySearch capabilitySearch(CapabilityIndex index,
                                             @Value("${agentflow.capability.cache-size:200}") int cacheSize,
                                             @Value("${agentflow.capability.cache-minutes:10}") int cacheMinutes) {
        return new CapabilitySearch(index, cacheSize, cacheMinutes);
    }

    /** Creates the capability table if the PgVector backend is used. */
    private void ensureTable(DataSource dataSource, String table) {
        String ddl = "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "name VARCHAR(255) PRIMARY KEY,"
                + "description TEXT,"
                + "method_ref VARCHAR(512),"
                + "source VARCHAR(64),"
                + "type VARCHAR(64),"
                + "allowed_tenants TEXT,"
                + "score DOUBLE PRECISION)";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(ddl);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create capability table " + table, e);
        }
    }
}
