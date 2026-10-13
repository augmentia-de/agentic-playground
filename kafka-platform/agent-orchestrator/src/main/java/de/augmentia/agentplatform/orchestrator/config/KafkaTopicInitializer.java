package de.augmentia.agentplatform.orchestrator.config;

import de.augmentia.agentplatform.common.kafka.KafkaTopics;
import de.augmentia.agentplatform.common.kafka.TopicSpec;
import de.augmentia.agentplatform.orchestrator.db.JdbcAgentDefinitionStore;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class KafkaTopicInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicInitializer.class);

    private final KafkaAdmin kafkaAdmin;
    private final OrchestratorProperties properties;
    private final org.springframework.beans.factory.ObjectProvider<JdbcAgentDefinitionStore> agentStore;

    public KafkaTopicInitializer(KafkaAdmin kafkaAdmin, OrchestratorProperties properties,
                                 org.springframework.beans.factory.ObjectProvider<JdbcAgentDefinitionStore> agentStore) {
        this.kafkaAdmin = kafkaAdmin;
        this.properties = properties;
        this.agentStore = agentStore;
    }

    @Override
    public void run(ApplicationArguments args) {
        // Configured types plus whatever is already registered in the database. An agent
        // imported through /agents/convert or auto-registered by /from-quad is not in the
        // configured list, and its task topic would otherwise be created by the broker on
        // first produce — with the broker default of ONE partition, so that agent type
        // processes its tasks strictly one after another while every other type gets 12.
        java.util.TreeSet<String> agentTypes = new java.util.TreeSet<>(properties.kafka().agentTypes());
        JdbcAgentDefinitionStore store = agentStore.getIfAvailable();
        if (store != null) {
            try {
                agentTypes.addAll(store.allAgentTypes());
            } catch (Exception ex) {
                log.warn("Could not read registered agent types from the database; "
                        + "their task topics are not provisioned at startup: {}", ex.toString());
            }
        }
        ensureTopicsFor(agentTypes);
    }

    /**
     * Creates and reconciles the task topics for the given agent types.
     *
     * <p>Called at start-up and again after agents are registered, so an imported agent gets
     * the specified partition count instead of inheriting the broker default. Idempotent:
     * existing topics are only raised to the target partition count, never shrunk.
     */
    public void ensureTopicsFor(java.util.Collection<String> agentTypes) {
        List<TopicSpec> specs = KafkaTopics.all(agentTypes);
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Set<String> existing = adminClient.listTopics().names().get();

            // 1) Fehlende Topics anlegen
            List<NewTopic> missing = specs.stream()
                    .filter(spec -> !existing.contains(spec.name()))
                    .map(KafkaTopicInitializer::toNewTopic)
                    .toList();
            if (!missing.isEmpty()) {
                adminClient.createTopics(missing).all().get();
                log.info("Created Kafka topics: {}", missing.stream().map(NewTopic::name).toList());
            }

            // 2) Partition-Zahl bei VORHANDENEN Topics durchsetzen.
            //
            // create-if-missing is not enough. Whoever publishes first wins — and
            // whoever publishes on a reused broker inherits 1 partition.
            // Measured in integration-tests/README.md: agent.execution.intake and
            // .results sat at 1 instead of 12 partitions, the platform was
            // serialised. That is not a theoretical risk but the state after every
            // broker restart without the initializer scripts.
            reconcilePartitions(adminClient, specs, existing);

            // 3) cleanup.policy durchsetzen.
            //
            // agent.config.updates is a COMPACT topic. Without this catch-up it
            // stays on "delete" if the topic first came into being without config —
            // the compaction the topic exists for never happens, and the topic
            // grows without bound.
            reconcileConfigs(adminClient, specs, existing);
        } catch (Exception ex) {
            log.warn("Kafka topic initialization failed; continuing because broker may create topics lazily", ex);
        }
    }

    /**
     * Raises the partition count of existing topics to the target value.
     *
     * <p><b>Kafka can only increase partitions, never shrink them.</b> If a topic is
     * above the target, that is logged and not corrected — shrinking would destroy
     * data and cannot be undone.
     */
    private void reconcilePartitions(AdminClient adminClient, List<TopicSpec> specs, Set<String> existing) {
        Map<String, Integer> actual = describePartitions(adminClient, specs, existing);
        Map<String, NewPartitions> wanted = new LinkedHashMap<>();
        List<String> tooMany = new ArrayList<>();

        for (TopicSpec spec : specs) {
            if (!existing.contains(spec.name())) continue;
            Integer current = actual.get(spec.name());
            if (current == null) continue;
            if (current < spec.partitions()) {
                wanted.put(spec.name(), NewPartitions.increaseTo(spec.partitions()));
            } else if (current > spec.partitions()) {
                tooMany.add(spec.name() + "=" + current + " (should be " + spec.partitions() + ")");
            }
        }

        if (!wanted.isEmpty()) {
            try {
                adminClient.createPartitions(wanted).all().get();
                log.info("Raised partition count to match spec on {} topic(s): {}", wanted.keySet(),
                        specs.stream().filter(s -> wanted.containsKey(s.name()))
                                .map(s -> s.name() + "=" + s.partitions()).toList());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while raising partition counts on " + wanted.keySet()
                        + " — the platform may stay serialised on these topics", e);
            } catch (Exception e) {
                // No abort: a broker that does not allow createPartitions must not
                // prevent startup. It does prevent scaling though — hence the warning.
                log.warn("Could not raise partition counts on " + wanted.keySet()
                        + " — without this the platform is serialised per topic", e);
            }
        }
        for (String t : tooMany) {
            // Deliberately only a warning: Kafka cannot shrink partitions.
            log.warn("Topic {} has more partitions than configured — Kafka cannot shrink a "
                    + "topic, raising is one-way. Recreate the topic if the count matters.", t);
        }
    }

    /**
     * Enforces {@code cleanup.policy} (and retention) on existing topics.
     *
     * <p>Deliberately does not throw when the server does not know the option: a broker
     * without log compaction must not prevent the orchestrator from starting.
     */
    private void reconcileConfigs(AdminClient adminClient, List<TopicSpec> specs, Set<String> existing) {
        Map<String, Map<String, String>> actual = describeConfigs(adminClient, specs, existing);
        Map<ConfigResource, Collection<AlterConfigOp>> changes = new LinkedHashMap<>();
        List<String> changedWhat = new ArrayList<>();

        for (TopicSpec spec : specs) {
            if (!existing.contains(spec.name())) continue;
            Map<String, String> have = actual.getOrDefault(spec.name(), Map.of());

            List<AlterConfigOp> ops = new ArrayList<>();
            for (var e : wantedConfigs(spec).entrySet()) {
                if (!e.getValue().equals(have.get(e.getKey()))) {
                    ops.add(new AlterConfigOp(
                            new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET));
                    changedWhat.add(spec.name() + "/" + e.getKey() + ": "
                            + have.getOrDefault(e.getKey(), "<unset>") + " -> " + e.getValue());
                }
            }
            if (!ops.isEmpty()) {
                changes.put(new ConfigResource(ConfigResource.Type.TOPIC, spec.name()), ops);
            }
        }

        if (changes.isEmpty()) return;
        try {
            // Kafka 4.x removed alterConfigs (full replace); only
            // incrementalAlterConfigs remains. That is safer here anyway: SET changes
            // exactly the named keys and leaves the rest of the topic untouched.
            adminClient.incrementalAlterConfigs(changes).all().get();
            log.info("Altered topic configs: {}", changedWhat);
        } catch (Exception ex) {
            log.warn("Could not alter topic configs " + changedWhat
                    + " — continuing. A 'delete' policy on a compacted topic means it grows "
                    + "unbounded, so this is worth investigating.", ex);
        }
    }

    private Map<String, String> wantedConfigs(TopicSpec spec) {
        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", spec.compacted() ? "compact" : "delete");
        if (!spec.compacted() && spec.retention() != null) {
            configs.put("retention.ms", String.valueOf(spec.retention().toMillis()));
        }
        return configs;
    }

    private Map<String, Integer> describePartitions(AdminClient adminClient, List<TopicSpec> specs,
                                                     Set<String> existing) {
        Map<String, Integer> result = new HashMap<>();
        List<String> names = specs.stream().map(TopicSpec::name).filter(existing::contains).toList();
        if (names.isEmpty()) return result;
        try {
            var described = adminClient.describeTopics(names).allTopicNames().get();
            described.forEach((name, d) -> result.put(name, d.partitions().size()));
        } catch (Exception ex) {
            log.warn("Could not describe topics to check partition counts: {}", ex.toString());
        }
        return result;
    }

    private Map<String, Map<String, String>> describeConfigs(AdminClient adminClient, List<TopicSpec> specs,
                                                            Set<String> existing) {
        Map<String, Map<String, String>> result = new HashMap<>();
        List<String> names = specs.stream().map(TopicSpec::name).filter(existing::contains).toList();
        if (names.isEmpty()) return result;
        try {
            var described = adminClient.describeConfigs(
                            names.stream().map(n -> new ConfigResource(
                                    ConfigResource.Type.TOPIC, n)).toList())
                    .all().get();
            described.forEach((resource, cfg) -> {
                Map<String, String> flat = new HashMap<>();
                for (ConfigEntry e : cfg.entries()) {
                    // isDefault(): the value comes from the broker default, not from
                    // the topic. Only explicitly set values count as "have".
                    if (!e.isDefault()) flat.put(e.name(), e.value());
                }
                result.put(resource.name(), flat);
            });
        } catch (Exception ex) {
            log.warn("Could not describe topic configs: {}", ex.toString());
        }
        return result;
    }

    private static NewTopic toNewTopic(TopicSpec spec) {
        NewTopic topic = new NewTopic(spec.name(), spec.partitions(), (short) 1);
        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", spec.compacted() ? "compact" : "delete");
        if (!spec.compacted() && spec.retention() != null) {
            configs.put("retention.ms", String.valueOf(spec.retention().toMillis()));
        }
        return topic.configs(configs);
    }
}
