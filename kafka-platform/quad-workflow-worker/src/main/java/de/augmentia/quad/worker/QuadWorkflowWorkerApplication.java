package de.augmentia.quad.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;

/**
 * Worker main class.
 *
 * <p><b>Why {@code DataSourceAutoConfiguration} is excluded:</b> the
 * worker brings {@code spring-boot-starter-jdbc} because in the mode
 * {@code worker.quad.agent-source=db} it reads the agent definitions from the shared
 * Postgres. Without the exclusion list Spring Boot in
 * default YAML mode (without a database) nevertheless tries to build a DataSource and the
 * startup fails with {@code 'url' attribute is not specified} — although
 * {@code JdbcAgentConfigSource} sits behind an {@code @ConditionalOnProperty}
 * and is not needed at all.
 *
 * <p>The exclusion is harmless because {@code DataSourceAutoConfiguration}
 * only creates a bean if none exists anyway. In DB mode
 * {@code QuadWorkerConfig.quadAgentDataSource()} supplies the bean explicitly; {@code JdbcTemplate}
 * and the agent config source build on it.
 */
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class QuadWorkflowWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(QuadWorkflowWorkerApplication.class, args);
    }
}