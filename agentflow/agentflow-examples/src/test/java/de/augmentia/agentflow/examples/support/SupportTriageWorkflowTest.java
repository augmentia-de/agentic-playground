package de.augmentia.agentflow.examples.support;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SupportTriageWorkflowTest {

    private static final String QUEUE = "agentflow";

    private TestWorkflowEnvironment env;
    private RecordingSupport activities;

    /** Hand-written activity double with a configurable classifier result. */
    static class RecordingSupport implements SupportActivities {
        final List<String> calls = new ArrayList<>();
        String assessment;

        RecordingSupport(String assessment) {
            this.assessment = assessment;
        }

        @Override
        public String assessTicket(String message) {
            calls.add("assess");
            return assessment;
        }

        @Override
        public String recallCustomer(String customerId) {
            calls.add("recall:" + customerId);
            return "";
        }

        @Override
        public String rememberTicket(String customerId, String summary) {
            calls.add("remember:" + customerId);
            return "t-" + customerId;
        }

        @Override
        public String findPlaybook(String topic) {
            calls.add("playbook");
            return "readFile";
        }
    }

    @BeforeEach
    void setUp() {
        env = TestWorkflowEnvironment.newInstance();
    }

    @AfterEach
    void tearDown() {
        if (env != null) {
            env.close();
        }
    }

    private void startWorker(String assessment) {
        Worker worker = env.newWorker(QUEUE);
        worker.registerWorkflowImplementationTypes(SupportTriageWorkflow.Impl.class);
        activities = new RecordingSupport(assessment);
        worker.registerActivitiesImplementations(activities);
        env.start();
    }

    private SupportTriageWorkflow newWorkflow(String workflowId) {
        return env.getWorkflowClient().newWorkflowStub(SupportTriageWorkflow.class,
                WorkflowOptions.newBuilder().setTaskQueue(QUEUE).setWorkflowId(workflowId).build());
    }

    @Test
    void highPriorityWaitsForEscalationSignal() {
        startWorker("{\"priority\":\"HIGH\"}");

        SupportTriageWorkflow workflow = newWorkflow("wf-high");
        WorkflowClient.start(workflow::run, "cust-1", "urgent issue");
        workflow.escalate(true);
        String result = WorkflowStub.fromTyped(workflow).getResult(String.class);

        assertThat(result).contains("escalated=true");
        assertThat(activities.calls).containsExactly("recall:cust-1", "assess", "playbook", "remember:cust-1");
    }

    @Test
    void lowPriorityIsHandledWithoutSignal() {
        startWorker("{\"priority\":\"LOW\"}");

        String result = newWorkflow("wf-low").run("cust-2", "minor question");

        assertThat(result).contains("handled");
    }
}
