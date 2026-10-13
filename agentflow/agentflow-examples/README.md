# AgentFlow Examples — five sample business cases

This module contains five end-to-end business cases that combine the AgentFlow capabilities.
It is a **library** (no separate boot jar): `agentflow-app` scans `de.augmentia.agentflow`,
so the examples' workflows/activities are served by the same Temporal worker and reachable
via REST.

> Prerequisite: Temporal (`agentflow/deploy`) and a reachable LLM (OpenCode Go / OpenAI).
> See `../README.md` (build, run, provider configuration).

## Cases at a glance

| # | Case | Endpoint | Composed capabilities |
|---|---|---|---|
| 1 | **Support-Triage** | `POST /examples/support-triage` | Structured Output, Memory, Capability/RAG, HITL |
| 2 | **Contract-Review** | `POST /examples/contract-review` | PII anonymization, Guardrail, Structured Output, HITL |
| 3 | **Invoice-Processing** | `POST /examples/invoice` | Saga/compensation (durable), Structured Output |
| 4 | **Market-Research** | `POST /examples/market-research` | Sub-agents, web tools |
| 5 | **Onboarding** | `POST /examples/onboarding` | Long-term Memory, capability search, MCP |

```bash
curl http://localhost:8095/examples      # Liste aller Cases (maschinenlesbar)
```

## 1 — Support-Triage (Structured Output + Memory + RAG + HITL)

Classifies a ticket into a typed `TicketAssessment` (category/priority/sentiment/
suggested reply), enriches it with customer memory and a capability/RAG lookup, and pauses
durably for a human escalation decision when the priority is **HIGH**.

```bash
curl -X POST "http://localhost:8095/examples/support-triage?customerId=c-1&message=Meine%20Rechnung%20ist%20falsch"
# falls HIGH: durabler Wartezustand
curl -X POST "http://localhost:8095/examples/support-triage/<workflowId>/escalate?approved=true"
```

Deterministic rule: `TriagePolicy.requiresEscalation(json)` (tested).

## 2 — Contract-Review (PII + Guardrail + HITL)

The agent reviews the contract text; emails/phone/credit cards are masked before the model
(`PiiAnonymizerHook`), prompt injection is blocked (`Guardrail`). Afterwards the workflow waits
durably for approval.

```bash
curl -X POST "http://localhost:8095/examples/contract-review?contract=<text>"
curl -X POST "http://localhost:8095/examples/contract-review/<workflowId>/approve?approved=false"
```

## 3 — Invoice-Processing (durable Saga)

Forward: `reserveBudget → postLedger → schedulePayment`. If the payment fails
(amount > `InvoicePolicy.DUAL_APPROVAL_THRESHOLD` = 100), the workflow compensates in
reverse order: `reverseLedger → releaseBudget`. Compensation is deterministic
workflow code and survives worker restarts.

```bash
curl -X POST "http://localhost:8095/examples/invoice?invoiceId=inv-1&amount=500"   # → compensated
curl -X POST "http://localhost:8095/examples/invoice?invoiceId=inv-2&amount=50"    # → paid
```

Tested with `TestWorkflowEnvironment`: `InvoiceWorkflowTest` checks the exact
call order including compensation.

## 4 — Market-Research (sub-agents)

An orchestrator agent delegates the research to a `researcher` sub-agent (web tools),
and a second agent writes an executive brief from it.

```bash
curl -X POST "http://localhost:8095/examples/market-research?topic=Temporal%20vs%20Kafka"
```

## 5 — Onboarding (Memory + Capability-Search + MCP)

Recalls project context (memory), creates a personal onboarding plan and can list the tools
of an external MCP server (SSE).

```bash
curl -X POST "http://localhost:8095/examples/onboarding?newHire=Ada&projectScope=agentflow"
```

## Structure

```
examples/
├── support/     (TicketAssessment, TriagePolicy, SupportActivities[+Impl], SupportTriageWorkflow)
├── contract/    (ContractReview, ContractActivities[+Impl], ContractReviewWorkflow)
├── invoice/     (ParsedInvoice, InvoicePolicy, InvoiceActivities[+Impl], InvoiceWorkflow)
├── research/    (ResearchActivities[+Impl], MarketResearchWorkflow)
├── onboarding/  (OnboardingPolicy, OnboardingActivities[+Impl], OnboardingWorkflow)
├── web/         (ExamplesController — REST entry point)
├── BusinessCases.java  (catalog of the cases)
└── Examples.java       (shared agent/hook helpers)
```

## Declarative definitions (quad.workflow v2)

The same five cases **additionally** exist as declarative v2 documents under
`agentflow-app/src/main/resources/workflows/*.json` (each with an embedded `agents[]`).
They are validated at startup with `quad-workflow-core` and are visible via
`GET /agentflow/workflows`; run them with
`POST /agentflow/runs/declarative?workflowId=<id>&task=<...>`.
See `../README.md` → "Declarative workflow definitions".

## Tests

```bash
cd agentflow && mvn -pl agentflow-examples -am test
```

- `TriagePolicyTest`, `InvoicePolicyTest`, `BusinessCasesTest` — deterministic rules.
- `InvoiceWorkflowTest`, `SupportTriageWorkflowTest` — durable workflows via
  `TestWorkflowEnvironment` with hand-written activity doubles (including the compensation and
  signal paths).
