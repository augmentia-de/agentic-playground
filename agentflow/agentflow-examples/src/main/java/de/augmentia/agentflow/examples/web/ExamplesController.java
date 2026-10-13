package de.augmentia.agentflow.examples.web;

import de.augmentia.agentflow.examples.BusinessCases;
import de.augmentia.agentflow.examples.contract.ContractReviewWorkflow;
import de.augmentia.agentflow.examples.invoice.InvoiceWorkflow;
import de.augmentia.agentflow.examples.onboarding.OnboardingWorkflow;
import de.augmentia.agentflow.examples.research.MarketResearchWorkflow;
import de.augmentia.agentflow.examples.support.SupportTriageWorkflow;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * REST entry points for the five sample business cases. Each endpoint starts a durable
 * Temporal workflow on the shared {@code agentflow} task queue (served by agentflow-app).
 */
@RestController
public class ExamplesController {

    static final String TASK_QUEUE = "agentflow";

    private final WorkflowClient workflowClient;

    public ExamplesController(WorkflowClient workflowClient) {
        this.workflowClient = workflowClient;
    }

    /** Lists the available business cases. */
    @GetMapping("/examples")
    public List<BusinessCases.Case> cases() {
        return BusinessCases.all();
    }

    // ── Case 1: Support triage ───────────────────────────────────────────────

    @PostMapping("/examples/support-triage")
    public Map<String, Object> supportTriage(@RequestParam("customerId") String customerId,
                                             @RequestParam("message") String message) {
        String workflowId = "ex-support-" + UUID.randomUUID();
        SupportTriageWorkflow workflow = workflowClient.newWorkflowStub(
                SupportTriageWorkflow.class, options(workflowId));
        WorkflowClient.start(workflow::run, customerId, message);
        return started(workflowId, "HIGH priority tickets wait for POST /examples/support-triage/{id}/escalate");
    }

    @PostMapping("/examples/support-triage/{workflowId}/escalate")
    public Map<String, Object> escalate(@PathVariable("workflowId") String workflowId,
                                        @RequestParam("approved") boolean approved) {
        workflowClient.newWorkflowStub(SupportTriageWorkflow.class, workflowId).escalate(approved);
        return Map.of("workflowId", workflowId, "signalled", "escalate", "approved", approved);
    }

    // ── Case 2: Contract review ──────────────────────────────────────────────

    @PostMapping("/examples/contract-review")
    public Map<String, Object> contractReview(@RequestParam("contract") String contract) {
        String workflowId = "ex-contract-" + UUID.randomUUID();
        ContractReviewWorkflow workflow = workflowClient.newWorkflowStub(
                ContractReviewWorkflow.class, options(workflowId));
        WorkflowClient.start(workflow::run, contract);
        return started(workflowId, "approve via POST /examples/contract-review/{id}/approve");
    }

    @PostMapping("/examples/contract-review/{workflowId}/approve")
    public Map<String, Object> approve(@PathVariable("workflowId") String workflowId,
                                       @RequestParam("approved") boolean approved) {
        workflowClient.newWorkflowStub(ContractReviewWorkflow.class, workflowId).approve(approved);
        return Map.of("workflowId", workflowId, "signalled", "approve", "approved", approved);
    }

    // ── Case 3: Invoice processing ───────────────────────────────────────────

    @PostMapping("/examples/invoice")
    public Map<String, Object> invoice(@RequestParam("invoiceId") String invoiceId,
                                       @RequestParam("amount") double amount) {
        String workflowId = "ex-invoice-" + UUID.randomUUID();
        InvoiceWorkflow workflow = workflowClient.newWorkflowStub(
                InvoiceWorkflow.class, options(workflowId));
        WorkflowClient.start(workflow::run, invoiceId, amount);
        return Map.of("workflowId", workflowId, "started", true,
                "hint", "amount > 100 triggers compensation");
    }

    // ── Case 4: Market research ──────────────────────────────────────────────

    @PostMapping("/examples/market-research")
    public Map<String, Object> marketResearch(@RequestParam("topic") String topic) {
        String workflowId = "ex-research-" + UUID.randomUUID();
        MarketResearchWorkflow workflow = workflowClient.newWorkflowStub(
                MarketResearchWorkflow.class, options(workflowId));
        WorkflowClient.start(workflow::run, topic);
        return started(workflowId, "orchestrator delegates to a research sub-agent");
    }

    // ── Case 5: Onboarding ───────────────────────────────────────────────────

    @PostMapping("/examples/onboarding")
    public Map<String, Object> onboarding(@RequestParam("newHire") String newHire,
                                          @RequestParam("projectScope") String projectScope) {
        String workflowId = "ex-onboarding-" + UUID.randomUUID();
        OnboardingWorkflow workflow = workflowClient.newWorkflowStub(
                OnboardingWorkflow.class, options(workflowId));
        WorkflowClient.start(workflow::run, newHire, projectScope);
        return started(workflowId, "uses project memory; MCP tools optional");
    }

    private WorkflowOptions options(String workflowId) {
        return WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).setWorkflowId(workflowId).build();
    }

    private Map<String, Object> started(String workflowId, String hint) {
        return Map.of("workflowId", workflowId, "started", true, "hint", hint);
    }
}
