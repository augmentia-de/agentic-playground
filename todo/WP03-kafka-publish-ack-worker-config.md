# WP03 — Kafka: publish/ack & worker config

| | |
|---|---|
| **Status** | open |
| **Severity** | Critical |
| **Modules** | `kafka-platform/quad-workflow-worker` |
| **Review reference** | B2, B37, B38, B39, B40 (§5.1, §5.3) |
| **Depends on** | – |
| **Estimated effort** | 1–2 days |

## Goal

A work result counts as "processed" only once it is **confirmed** to be in Kafka.
Additionally: consistent configuration, correct metrics, no duplicate beans.

## Findings (as-is)

- **B2 – ack before confirmed publish:** `WorkflowResultPublisher.send` and `WorkflowDlqPublisher.send`
  use asynchronous `producer.send(...)` without `.get()`/flush; `TaskExecutor.safeExecute` then
  returns `true` immediately; `WorkflowTaskListener` acks
  (`WorkflowResultPublisher.java:36-41`, `WorkflowDlqPublisher.java:46`,
  `TaskExecutor.java:140-183`, `WorkflowTaskListener.java:107-132`). No shutdown flush.
- **B37 – metric mismatch:** `WorkerMetrics` registers `…completed_total{status="failed"}`, while the
  failure path increments `…failed_total` (`:46-49,90-94`).
- **B38 – duplicate beans:** `GracefulShutdown`, `WorkerMetrics` each as `@Component` **and** `@Bean`
  (`QuadWorkerConfig.java:428-455`) → only bootable because of `allow-bean-definition-overriding: true`
  (`application.yml:20`).
- **B39 – inconsistent bootstrap defaults:** producer `localhost:9092` (`QuadWorkerConfig.java:52-58`)
  vs. consumer `kafka:9092` (`KafkaConfig.java:26`); non-transactional producer with
  `read_committed` (`KafkaConfig.java:61`).
- **B40 – `worker.quad.concurrency` unused:** bound but never read
  (`application.yml:51`, real limit `TaskExecutor.java:97`).

## 1. Tests first (Red)

Test location: `kafka-platform/quad-workflow-worker/src/test/java/de/augmentia/quad/worker/…`
(existing harness: `WorkflowTaskListenerTest`, `WorkerTestFixtures`, `QuadIntegrationContractTest`).

| # | Test | Expectation |
|---|------|-------------|
| 1 | `ResultPublisherAckTest#ackOnlyAfterSendConfirmed` | `safeExecute` returns `true` only once the publish is confirmed |
| 2 | `ResultPublisherAckTest#sendFailure_returnsFalse_noAck` | publish fails → `false` → no ack |
| 3 | `ResultPublisherAckTest#flushOnShutdown` | buffered messages are flushed on shutdown |
| 4 | `WorkerMetricsTest#failureIncrementsRegisteredCounter` | failure increments **exactly** the exported counter |
| 5 | `WorkerContextTest#noDuplicateBeans` | context starts even without `allow-bean-definition-overriding` |
| 6 | `KafkaConfigTest#bootstrapDefaultsConsistent` | producer and consumer defaults are identical |
| 7 | `WorkerPropertiesBindingTest#concurrency_isApplied` | `concurrency` drives listener parallelism |

**Acceptance (Red):** 1, 2, 4, 5, 7 fail.

## 2. Changes (Green)

1. **Confirmed send:** `WorkflowResultPublisher`/`WorkflowDlqPublisher` wait for the confirmation
   (`.get(timeout)` or callback + `CountDownLatch`); `TaskExecutor` returns `true` only after a
   successful confirmation. `KafkaTemplate.flush()` in a shutdown hook (`GracefulShutdown`).
2. **Metric:** use one counter per status (`completed_total{status=…}`), no two-sided registration;
   use fields instead of re-`register`.
3. **Beans:** remove duplicate definitions; set `allow-bean-definition-overriding` to `false`.
4. **Config:** a shared bootstrap default (property with a clear default); make the producer
   transactional/replay-consistent with `read_committed`, or adjust `isolation.level`.
5. **`concurrency`:** either set listener concurrency from it **or** remove the property and fix the docs.

## 3. Verification

```bash
mvn -pl kafka-platform/quad-workflow-worker -am test
cd kafka-platform && mvn -pl quad-workflow-worker test
```
- Load test: simulate producer failure (briefly stop the broker) → the record is redelivered, no loss.
- Shutdown test: stop the worker with pending messages → no losses in the log.

## 4. Risks / notes

- Synchronous `.get()` reduces throughput; weigh timeout and batch size
  (alternative: callback + semaphore per record).
- Redelivery requires idempotency on the orchestrator side (present, but verify).

## Definition of Done

- [ ] Ack only after a confirmed publish (tests 1–3 green).
- [ ] Shutdown flush present.
- [ ] Metric unambiguous and exported (test 4).
- [ ] No duplicate beans, no `allow-bean-definition-overriding` (test 5).
- [ ] Bootstrap/isolation consistency (test 6), `concurrency` effective or removed (test 7).
- [ ] Progress log maintained.

## Progress log

| Date | Author | Step | Result |
|------|--------|------|--------|
| | | 1. Tests red | |
| | | 2. Implementation | |
| | | 3. Verification | |
