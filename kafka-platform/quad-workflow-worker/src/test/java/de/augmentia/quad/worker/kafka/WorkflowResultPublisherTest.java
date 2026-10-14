package de.augmentia.quad.worker.kafka;

import de.augmentia.agentplatform.common.event.ResultEvent;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * WP03/B2 — the result publisher must not report success before the broker confirmed the write.
 */
class WorkflowResultPublisherTest {

    private static ResultEvent result() {
        return new ResultEvent(new ResultEvent.Header(null, 0L),
            "wf-1", "task-1", "step-1", 0, 0, 0L, 0L, Map.of("status", "COMPLETED"));
    }

    @Test
    void sendWaitsForBrokerConfirmation() throws Exception {
        @SuppressWarnings("unchecked")
        Producer<String, String> producer = mock(Producer.class);
        CompletableFuture<RecordMetadata> confirmed = new CompletableFuture<>();
        when(producer.send(any(ProducerRecord.class))).thenReturn(confirmed);

        var publisher = new WorkflowResultPublisher(producer, "agent.execution.results", 5000);

        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            try {
                publisher.send("wf-1", result());
            } catch (Throwable e) {
                error.set(e);
            } finally {
                done.countDown();
            }
        });
        t.setDaemon(true);
        t.start();

        // while the broker has not confirmed, send() must still be blocked (not yet "delivered")
        assertThat(done.await(200, TimeUnit.MILLISECONDS))
            .as("send() must block until the broker confirms")
            .isFalse();

        confirmed.complete(null); // broker confirmation

        assertThat(done.await(2, TimeUnit.SECONDS)).as("send() unblocks after confirmation").isTrue();
        assertThat(error.get()).isNull();
        verify(producer).send(any(ProducerRecord.class));
    }

    @Test
    void rejectedSendThrowsKafkaPublishException() {
        @SuppressWarnings("unchecked")
        Producer<String, String> producer = mock(Producer.class);
        CompletableFuture<RecordMetadata> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker down"));
        when(producer.send(any(ProducerRecord.class))).thenReturn(failed);

        var publisher = new WorkflowResultPublisher(producer, "agent.execution.results", 1000);

        assertThatThrownBy(() -> publisher.send("wf-1", result()))
            .isInstanceOf(KafkaPublishException.class)
            .hasMessageContaining("wf-1");
    }

    @Test
    void unconfirmedSendTimesOut() {
        @SuppressWarnings("unchecked")
        Producer<String, String> producer = mock(Producer.class);
        when(producer.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<>());

        var publisher = new WorkflowResultPublisher(producer, "agent.execution.results", 50);

        assertThatThrownBy(() -> publisher.send("wf-1", result()))
            .isInstanceOf(KafkaPublishException.class)
            .hasMessageContaining("Timed out");
    }
}
