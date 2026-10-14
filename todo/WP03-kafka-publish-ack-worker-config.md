# WP03 — Kafka: publish/ack & worker config

| | |
|---|---|
| **Status** | done |
| **Severity** | Critical |
| **Modules** | `kafka-platform/quad-workflow-worker` |
| **Review reference** | B2, B37, B38, B39, B40 (§5.1, §5.3) |
| **Depends on** | – |
| **Estimated effort** | 1–2 days |

## Goal

A work result counts as "processed" only once it is **confirmed** to be in Kafka.
Additionally: consistent configuration, correct metrics, no duplicate beans.

## Findings (as-is)

- **B2 – ack before confirmed publish:** `WorkflowResultPublisher.send` used
  `producer.send(record, callback)` (fire-and-forget) and `WorkflowDlqPublisher.send` used
  `producer.send(record)` without any confirmation. `TaskExecutor.safeExecute` therefore returned
  `true` immediately and `WorkflowTaskListener` acknowledged
  (`WorkflowResultPublisher.java:36-41`, `WorkflowDlqPublisher.java:46`,
  `TaskExecutor.java:140-183`, `WorkflowTaskListener.java:107-132`). A broker outage between
  execution and publish lost the result of an already paid-for task without redelivery.
- **B37 – metric mismatch:** `WorkerMetrics` registered the failure series as
  `quad_worker_tasks_completed_total{status="failed"}`, but the failure path incremented
  `quad_worker_tasks_failed_total` (`WorkerMetrics.java:46-49,90-94`). The registered series stayed
  at zero; the `tasksCompleted`/`tasksFailed` fields were never read.
- **B38 – duplicate beans:** `GracefulShutdown` and `WorkerMetrics` were each declared both as
  `@Component` and as `@Bean` (`QuadWorkerConfig.java:428-455`) → only bootable because of
  `allow-bean-definition-overriding: true` (`application.yml:20`).
- **B39 – inconsistent bootstrap defaults:** producer default `localhost:9092`
  (`QuadWorkerConfig.java:52-58`) vs. consumer default `kafka:9092` (`KafkaConfig.java:26`); a
  non-transactional producer combined with `isolation.level=read_committed` (`KafkaConfig.java:61`).
- **B40 – `worker.quad.concurrency` unused:** bound in `WorkerProperties` / `application.yml:51`
  but never read (the real limit is `max-concurrent-tasks` in `TaskExecutor`).

## 1. Tests first (Red)

Test location: `kafka-platform/quad-workflow-worker/src/test/java/de/augmentia/quad/worker/…`

| # | Test | Expectation |
|---|------|-------------|
| 1 | `WorkflowResultPublisherTest#sendWaitsForBrokerConfirmation` | `send` blocks until the broker-confirmation future completes |
| 2 | `WorkflowResultPublisherTest#rejectedSendThrowsKafkaPublishException` | broker error → `KafkaPublishException` |
| 3 | `WorkflowResultPublisherTest#unconfirmedSendTimesOut` | no confirmation within the timeout → `KafkaPublishException` |
| 4 | `WorkflowDlqPublisherTest#dlqSendIsConfirmedAndTargetsTheDlqTopic` | DLQ write confirmed, correct topic/key/payload |
| 5 | `WorkflowDlqPublisherTest#rejectedDlqSendThrowsKafkaPublishException` | broker error → `KafkaPublishException` |
| 6 | `WorkerMetricsTest#failureIncrementsTheRegisteredOutcomeCounter` | failure increments `…completed_total{status="failed"}` |
| 7 | `WorkerMetricsTest#noSeparateFailedMetricIsEmitted` | no `…tasks_failed_total` is created |
| 8 | `WorkerMetricsTest#successAndFailureShareTheOutcomeMetric` | both outcomes on one metric, split by `status` |
| 9 | `WorkerMetricsTest#receivedDlqAndToolCountersAreExported` | the remaining counters are exported |
| 10 | `KafkaBootstrapConsistencyTest#consumerUsesSpringBootstrapWhenSet` | consumer honours `spring.kafka.bootstrap-servers` |
| 11 | `KafkaBootstrapConsistencyTest#consumerAndProducerUseTheSameBootstrap` | producer == consumer bootstrap |
| 12 | `KafkaBootstrapConsistencyTest#bootstrapDefaultsAreConsistent` | both default to `localhost:9092` |
| 13 | `KafkaBootstrapConsistencyTest#consumerDoesNotClaimReadCommittedWithoutATransactionalProducer` | no misleading `read_committed` |
| 14 | `WorkerBeanUniquenessTest#metricsAndShutdownAreNotAlsoDeclaredAsBeans` | no duplicate `@Bean` for the two components |
| 15 | `WorkerBeanUniquenessTest#metricsAndShutdownAreStillComponentScanned` | both remain `@Component` |
| 16 | `WorkerBeanUniquenessTest#shippedConfigDoesNotEnableBeanOverriding` | `allow-bean-definition-overriding` stays false |
| 17 | `WorkerPropertiesBindingTest#unusedConcurrencyPropertyIsGone` | `concurrency` is no longer a record component |

**Acceptance (Red):** tests 1–14 initially fail (B2 = no blocking, B37 = wrong metric name,
B38 = duplicate beans, B39 = differing defaults, B40 = field still present).

## 2. Changes (Green)

1. **Confirmed send (B2):** `WorkflowResultPublisher.send` and `WorkflowDlqPublisher.send` now
   block on `producer.send(record).get(sendTimeoutMs, MILLISECONDS)` and throw a new
   `KafkaPublishException` on broker error/timeout/interrupt. `TaskExecutor` already treats a throwing
   `results.send`/`dlq.send` as "not delivered" (returns `false` → no ack → redelivery). The send
   timeout is configurable via `worker.quad.kafka.send-timeout-ms` (default 30 s). Shutdown is safe:
   the `Producer` bean is `Closeable`, so Spring flushes/closes it on context teardown.
2. **Metric (B37):** `WorkerMetrics` reports all outcomes on the single
   `quad_worker_tasks_completed_total{agentType,status}` series; the failure reason is kept as the
   `errorClass` tag. The dead `tasksCompleted`/`tasksFailed` fields were removed.
3. **Beans (B38):** removed the duplicate `@Bean` definitions from `QuadWorkerConfig`; the two
   classes are provided by component scanning only. `allow-bean-definition-overriding` set to `false`.
4. **Config (B39):** a single `KafkaConfig.resolveBootstrapServers(env)` (precedence
   `spring.kafka.bootstrap-servers` → `KAFKA_BOOTSTRAP_SERVERS` → `localhost:9092`) feeds both the
   consumer and the manual producer. The misleading `isolation.level=read_committed` was removed
   (the producer is not transactional).
5. **`concurrency` (B40):** removed from `WorkerProperties` and `application.yml`; the record and all
   call sites were updated. A stray `worker.quad.concurrency` in a deployment is ignored by the binder.

## 3. Verification

```bash
mvn -pl kafka-platform/quad-workflow-worker -am test
cd kafka-platform && mvn -pl quad-workflow-worker test
```
- Load test (manual): stop the broker → the result is not confirmed → `safeExecute` returns `false`,
  the record is redelivered, no loss.
- Design note: the listener blocks the whole batch, so the synchronous confirm stays well below
  `max.poll.interval.ms` (30 s default vs. 900 s interval).

## 4. Risks / notes

- Synchronous `.get()` trades throughput for correctness; the timeout (30 s) and the permit
  semaphore in `TaskExecutor` bound the impact. A future optimisation could use a callback plus a
  per-record `CountDownLatch`, but the semantics must stay "ack only after confirmation".
- Aligning the default bootstrap to `localhost:9092` (instead of `kafka:9092`) only changes behaviour
  when no `KAFKA_BOOTSTRAP_SERVERS` / `spring.kafka.bootstrap-servers` is configured — which in
  production always is.

## Definition of Done

- [x] Ack only after a confirmed publish (tests 1–5).
- [x] Shutdown flush/closing covered by the `Closeable` producer bean.
- [x] Outcome metric unambiguous and exported (tests 6–9).
- [x] No duplicate beans, `allow-bean-definition-overriding` false (tests 14–16).
- [x] Bootstrap/isolation consistency (tests 10–13); `concurrency` removed (test 17).
- [x] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| 2026-10-14 | @opencode | 1. Tests | 17 new/updated cases across 5 test classes |
| 2026-10-14 | @opencode | 2. Implementation | confirmed send + `KafkaPublishException`; unified outcome metric; removed duplicate beans + disabled overriding; shared bootstrap resolver; dropped `read_committed` and `concurrency` |
| 2026-10-14 | @opencode | 3. Verification | `mvn -pl kafka-platform/quad-workflow-worker -am test` → BUILD SUCCESS (86 tests, 0 failures, 1 skipped) |
