package de.augmentia.agentflow.examples.invoice;

import io.temporal.client.WorkflowOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class InvoiceWorkflowTest {

    private static final String QUEUE = "agentflow";

    private TestWorkflowEnvironment env;
    private RecordingInvoice activities;

    /** Hand-written activity double that records the call order. */
    static class RecordingInvoice implements InvoiceActivities {
        final List<String> calls = new ArrayList<>();

        @Override
        public String extractInvoice(String text) {
            return "{}";
        }

        @Override
        public String reserveBudget(String invoiceId) {
            calls.add("reserve:" + invoiceId);
            return "res-" + invoiceId;
        }

        @Override
        public String postLedger(String invoiceId) {
            calls.add("post:" + invoiceId);
            return "posted:" + invoiceId;
        }

        @Override
        public String schedulePayment(String invoiceId, double amount) {
            calls.add("pay:" + invoiceId);
            if (amount > InvoicePolicy.DUAL_APPROVAL_THRESHOLD) {
                throw new IllegalStateException("payment declined");
            }
            return "scheduled:" + invoiceId;
        }

        @Override
        public void reverseLedger(String invoiceId) {
            calls.add("reverse:" + invoiceId);
        }

        @Override
        public void releaseBudget(String reservationId) {
            calls.add("release:" + reservationId);
        }
    }

    @BeforeEach
    void setUp() {
        env = TestWorkflowEnvironment.newInstance();
        Worker worker = env.newWorker(QUEUE);
        worker.registerWorkflowImplementationTypes(InvoiceWorkflow.Impl.class);
        activities = new RecordingInvoice();
        worker.registerActivitiesImplementations(activities);
        env.start();
    }

    @AfterEach
    void tearDown() {
        env.close();
    }

    private String run(String invoiceId, double amount) {
        InvoiceWorkflow workflow = env.getWorkflowClient().newWorkflowStub(
                InvoiceWorkflow.class, WorkflowOptions.newBuilder().setTaskQueue(QUEUE).build());
        return workflow.run(invoiceId, amount);
    }

    @Test
    void compensatesInReverseOrderWhenPaymentFails() {
        String result = run("inv-1", 500.0);

        assertThat(result).contains("compensated").contains("res-inv-1");
        assertThat(activities.calls).containsExactly(
                "reserve:inv-1", "post:inv-1", "pay:inv-1", "reverse:inv-1", "release:res-inv-1");
    }

    @Test
    void paysSmallAmountsWithoutCompensation() {
        String result = run("inv-2", 50.0);

        assertThat(result).contains("paid").contains("scheduled:inv-2");
        assertThat(activities.calls).containsExactly("reserve:inv-2", "post:inv-2", "pay:inv-2");
    }
}
