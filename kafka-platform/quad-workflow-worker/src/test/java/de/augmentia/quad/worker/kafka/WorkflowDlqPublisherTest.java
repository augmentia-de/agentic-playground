package de.augmentia.quad.worker.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * WP03/B2 — the DLQ publisher must confirm the write, otherwise a malformed record could be
 * acknowledged although its DLQ entry was never stored (a silent data-loss path).
 */
class WorkflowDlqPublisherTest {

    private static ConsumerRecord<String, String> record() {
        return new ConsumerRecord<>("agent.tasks.researcher", 0, 5L, "wf-1", "{ not json }");
    }

    @Test
    void dlqSendIsConfirmedAndTargetsTheDlqTopic() {
        @SuppressWarnings("unchecked")
        Producer<String, String> producer = mock(Producer.class);
        when(producer.send(any(ProducerRecord.class)))
            .thenReturn(CompletableFuture.<RecordMetadata>completedFuture(null));

        var publisher = new WorkflowDlqPublisher(producer, "agent.execution.dlq", 1000);
        publisher.send(record(), new IllegalArgumentException("bad json"));

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(captor.capture());

        ProducerRecord<String, String> sent = captor.getValue();
        assertThat(sent.topic()).isEqualTo("agent.execution.dlq");
        assertThat(sent.key()).isEqualTo("wf-1");
        assertThat(sent.value()).contains("agent.tasks.researcher").contains("bad json");
    }

    @Test
    void rejectedDlqSendThrowsKafkaPublishException() {
        @SuppressWarnings("unchecked")
        Producer<String, String> producer = mock(Producer.class);
        CompletableFuture<RecordMetadata> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker down"));
        when(producer.send(any(ProducerRecord.class))).thenReturn(failed);

        var publisher = new WorkflowDlqPublisher(producer, "agent.execution.dlq", 1000);

        assertThatThrownBy(() -> publisher.send(record(), new RuntimeException("x")))
            .isInstanceOf(KafkaPublishException.class);
    }
}
