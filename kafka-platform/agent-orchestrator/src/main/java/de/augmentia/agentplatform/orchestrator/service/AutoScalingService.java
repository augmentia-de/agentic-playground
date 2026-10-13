package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.orchestrator.config.OrchestratorProperties;
import de.augmentia.agentplatform.orchestrator.metrics.OrchestratorMetrics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@ConditionalOnProperty(name = "app.autoscaling.enabled", havingValue = "true")
public class AutoScalingService {

    private static final Logger log = LoggerFactory.getLogger(AutoScalingService.class);
    private static final String REDIS_SEMAPHORE_KEY = "autoscaling:semaphore";
    private static final String REDIS_CONSECUTIVE_DECREASE_KEY = "autoscaling:consecutive_decrease";
    private static final Duration SEMAPHORE_TTL = Duration.ofMinutes(1);

    private final OrchestratorProperties properties;
    private final ConsumerFactory<String, String> consumerFactory;
    private final StringRedisTemplate redisTemplate;
    private final OrchestratorMetrics metrics;

    @Autowired
    public AutoScalingService(OrchestratorProperties properties,
                              ConsumerFactory<String, String> consumerFactory,
                              StringRedisTemplate redisTemplate,
                              OrchestratorMetrics metrics) {
        this.properties = properties;
        this.consumerFactory = consumerFactory;
        this.redisTemplate = redisTemplate;
        this.metrics = metrics;
    }

    @Scheduled(fixedRateString = "${app.autoscaling.poll-interval-seconds:30}000")
    public void evaluateScale() {
        if (!properties.autoScaling().enabled()) {
            return;
        }

        OrchestratorProperties.AutoScaling cfg = properties.autoScaling();
        int lagThreshold = cfg.lagThreshold();
        int minWorkers = cfg.minWorkers();
        int maxWorkers = cfg.maxWorkers();

        try {
            Map<Integer, Long> partitionLags = computeTotalLag(cfg.lagTopic(), cfg.lagGroupId());
            int numPartitions = partitionLags.size();
            if (numPartitions == 0) {
                return;
            }

            double avgLagPerPartition = partitionLags.values().stream()
                    .mapToLong((longVal) -> longVal)
                    .average()
                    .orElse(0.0);

            metrics.gaugeWorkerLoadScore(avgLagPerPartition);

            evaluateWithLock(getCurrentScaleTarget(), avgLagPerPartition, cfg);
        } catch (Exception ex) {
            log.warn("Auto-scaling evaluation failed", ex);
        }
    }

    /**
     * The part of the evaluation that writes — and only that holds the lock.
     *
     * <p>The lock belongs around the read-and-write, not around the lag measurement: only
     * then can the evaluation be serialised. Without it every instance can scale up, and
     * the result would be that of the last writer instead of the sum.
     *
     * <p>If the lock is not acquired, nothing is written and nothing is released. An
     * unconditional release in the {@code finally} would release the lock of an instance
     * that currently holds it and is in the middle of its evaluation.
     */
    void evaluateWithLock(int currentTarget, double avgLagPerPartition,
                          OrchestratorProperties.AutoScaling cfg) {
        if (!acquireSemaphore()) {
            log.debug("Auto-scaling skipped — another instance is evaluating right now");
            return;
        }
        try {
            applyScaleDecision(currentTarget, avgLagPerPartition, cfg);
        } finally {
            releaseSemaphore();
        }
    }

    /**
     * Decides on the target value, writes it and publishes it.
     *
     * <p>Returns the target value that is actually set afterwards. That is the value the
     * metric has to report: previously this stood at {@code currentTarget}, that is, the
     * state from before the change, and the metric lagged one cycle behind after every
     * scaling step.
     *
     * <p>The caller must hold the lock.
     */
    void applyScaleDecision(int currentTarget, double avgLagPerPartition,
                            OrchestratorProperties.AutoScaling cfg) {
        int lagThreshold = cfg.lagThreshold();
        int minWorkers = cfg.minWorkers();
        int maxWorkers = cfg.maxWorkers();

        int effectiveTarget = currentTarget;
        if (currentTarget < maxWorkers && avgLagPerPartition > lagThreshold) {
            effectiveTarget = Math.min(currentTarget + 1, maxWorkers);
            log.info("Scaling up workers from {} to {} (avg lag={}, threshold={})",
                    currentTarget, effectiveTarget, (long) avgLagPerPartition, lagThreshold);
        } else if (currentTarget > minWorkers && avgLagPerPartition < lagThreshold / 2.0) {
            long consecutiveLow = getConsecutiveDecreaseCount();
            if (consecutiveLow >= 5) {
                effectiveTarget = Math.max(currentTarget - 1, minWorkers);
                setConsecutiveDecreaseCount(0);
                log.info("Scaling down workers from {} to {} (avg lag below half threshold)",
                        currentTarget, effectiveTarget);
            } else {
                setConsecutiveDecreaseCount(consecutiveLow + 1);
            }
        } else {
            setConsecutiveDecreaseCount(0);
        }

        if (effectiveTarget != currentTarget) {
            updateScaleTarget(effectiveTarget);
        }
        metrics.gaugeWorkerScaleTarget(effectiveTarget);
    }

    /**
     * Reads the bootstrap server from the consumer properties.
     *
     * <p>Casting to {@code String} is not permissible here: the Kafka client normalises
     * {@code bootstrap.servers} into a {@code List<String>}. The naive cast ends at runtime
     * in {@code ClassCastException: ArrayList cannot be cast to String} — and with it the
     * entire auto-scaling evaluation fails, not just the individual case.
     */
    static String firstBootstrapServer(Map<String, Object> props) {
        Object raw = props.get("bootstrap.servers");
        if (raw instanceof java.util.List<?> list && !list.isEmpty()) {
            return String.valueOf(list.get(0));
        }
        if (raw instanceof String s && !s.isBlank()) {
            return s;
        }
        return "localhost:9092";
    }

    /**
     * Admin config from the consumer properties.
     *
     * <p>Deliberately contains <em>only</em> {@code bootstrap.servers}. The rest of the
     * consumer properties do not belong in an AdminClient: Spring Boot puts neither
     * {@code group.id} nor deserialisers in there, and {@code group.id} is missing there in
     * production too — all listeners set their group on the {@code @KafkaListener}
     * annotation, and such overrides do not land in the factory properties.
     */
    static Map<String, Object> adminProperties(Map<String, Object> consumerProps) {
        return Map.of("bootstrap.servers", firstBootstrapServer(consumerProps));
    }

    /**
     * Lag per partition: end offset minus the group's committed offset.
     *
     * <p>No consumer needed — the AdminClient supplies partitions and offsets. That used to
     * be different and therefore broke in operation: a {@code KafkaConsumer} built from the
     * factory properties has no {@code group.id} and throws
     * {@code InvalidGroupIdException} on {@code subscribe}.
     *
     * <p><b>Not</b> {@code endOffset - beginningOffset}: that is the entire retained history
     * of the topic, not the lag. As soon as retention applies, the beginning offset is
     * greater than null and you are measuring the past.
     *
     * <p>If the group has never committed for a partition, the entry is missing entirely —
     * that counts as offset 0 and thus as "nothing consumed", which is the correct reading.
     */
    static Map<Integer, Long> lagPerPartition(Map<TopicPartition, Long> endOffsets,
                                              Map<TopicPartition, OffsetAndMetadata> committed) {
        Map<Integer, Long> lag = new HashMap<>();
        for (var entry : endOffsets.entrySet()) {
            OffsetAndMetadata offset = committed.get(entry.getKey());
            long committedOffset = (offset == null) ? 0L : offset.offset();
            lag.put(entry.getKey().partition(), Math.max(0L, entry.getValue() - committedOffset));
        }
        return lag;
    }

    private Map<Integer, Long> computeTotalLag(String topic, String groupId) {
        var adminProps = adminProperties(consumerFactory.getConfigurationProperties());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            return computeTotalLag(admin, topic, groupId);
        } catch (Exception ex) {
            log.error("Failed to open an admin client for {}", adminProps.get("bootstrap.servers"), ex);
            return Map.of();
        }
    }

    /**
     * Lag against an existing AdminClient.
     *
     * <p>Separate from {@link #computeTotalLag(String, String)} so that the broker
     * interaction is testable without a network. Errors are swallowed here as there and
     * reported as an empty map: the callers treat 0 partitions as "nothing to do", not as
     * an error.
     */
    Map<Integer, Long> computeTotalLag(AdminClient admin, String topic, String groupId) {
        try {
            Map<TopicPartition, Long> endOffsets = endOffsets(admin, topic);
            Map<TopicPartition, OffsetAndMetadata> committed = committedOffsets(admin, groupId);
            return lagPerPartition(endOffsets, committed);
        } catch (Exception ex) {
            log.error("Failed to compute consumer lag for topic {} group {}", topic, groupId, ex);
            return Map.of();
        }
    }

    /** End offsets of all partitions of the topic. Needs no consumer group. */
    private static Map<TopicPartition, Long> endOffsets(AdminClient admin, String topic) throws Exception {
        var partitions = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).partitions();
        if (partitions.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        partitions.forEach(info -> request.put(new TopicPartition(topic, info.partition()), OffsetSpec.latest()));
        var result = admin.listOffsets(request).all().get();
        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        result.forEach((tp, info) -> endOffsets.put(tp, info.offset()));
        return endOffsets;
    }

    /** Committed offsets of the group. A group that has never been active counts as "nothing consumed". */
    private static Map<TopicPartition, OffsetAndMetadata> committedOffsets(AdminClient admin, String groupId)
            throws Exception {
        try {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
            return committed == null ? Map.of() : committed;
        } catch (GroupIdNotFoundException ex) {
            // Caught directly and not via ExecutionException: KafkaFuture.get() propagates
            // RuntimeExceptions unwrapped. Waiting for ExecutionException here misses the
            // case and reports a group that never ran as an error.
            log.info("Consumer group {} has never committed — everything counts as lag", groupId);
            return Map.of();
        }
    }

    private int getCurrentScaleTarget() {
        String val = redisTemplate.opsForValue().get("autoscaling:target");
        if (val == null) {
            return properties.autoScaling().minWorkers();
        }
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException ex) {
            return properties.autoScaling().minWorkers();
        }
    }

    /**
 * Writes the target value with a TTL.
 *
 * <p>Without a prior comparison against the current state: the caller only calls this when
 * the value actually changed relative to the state just read
 * ({@code effectiveTarget != currentTarget}). A second read would see the same value,
 * return early and cost one Redis roundtrip per evaluation — at a 30-second cadence and up
 * to 20 instances that is pure load without effect.
 */
    private void updateScaleTarget(int target) {
        redisTemplate.opsForValue().set("autoscaling:target", String.valueOf(target), SEMAPHORE_TTL);
    }

    /**
     * Acquires the evaluation lock. {@code false} means: another instance is evaluating
     * right now, or Redis was unreachable.
     *
     * <p>The result is evaluated — the caller must not write anything without the lock. A
     * {@code null} from Redis is an error, not an acquire: then nothing is done, instead
     * of aborting the run and hiding the actual problem.
     */
    private boolean acquireSemaphore() {
        Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(REDIS_SEMAPHORE_KEY, "locked", SEMAPHORE_TTL);
        return Boolean.TRUE.equals(acquired);
    }

    private void releaseSemaphore() {
        redisTemplate.delete(REDIS_SEMAPHORE_KEY);
    }

    private long getConsecutiveDecreaseCount() {
        String val = redisTemplate.opsForValue().get(REDIS_CONSECUTIVE_DECREASE_KEY);
        return val == null ? 0 : Long.parseLong(val);
    }

    private void setConsecutiveDecreaseCount(long count) {
        redisTemplate.opsForValue().set(REDIS_CONSECUTIVE_DECREASE_KEY, String.valueOf(count), SEMAPHORE_TTL);
    }
}
