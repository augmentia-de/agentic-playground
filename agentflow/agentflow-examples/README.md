# AgentFlow Examples — fünf Sample-Business-Cases

Dieses Modul enthält fünf End-to-End-Business-Cases, die die AgentFlow-Fähigkeiten
kombinieren. Es ist eine **Bibliothek** (kein eigenes Boot-Jar): `agentflow-app` scannt
`de.augmentia.agentflow`, dadurch werden Workflows/Activities der Beispiele vom selben
Temporal-Worker bedient und über REST erreichbar.

> Voraussetzung: Temporal (`agentflow/deploy`) und ein erreichbares LLM (OpenCode Go / OpenAI).
> Siehe `../README.md` (Build, Run, Provider-Konfiguration).

## Cases im Überblick

| # | Case | Endpoint | Komponierte Fähigkeiten |
|---|---|---|---|
| 1 | **Support-Triage** | `POST /examples/support-triage` | Structured Output, Memory, Capability/RAG, HITL |
| 2 | **Contract-Review** | `POST /examples/contract-review` | PII-Anonymisierung, Guardrail, Structured Output, HITL |
| 3 | **Invoice-Processing** | `POST /examples/invoice` | Saga/Kompensation (durabel), Structured Output |
| 4 | **Market-Research** | `POST /examples/market-research` | Sub-Agenten, Web-Tools |
| 5 | **Onboarding** | `POST /examples/onboarding` | Long-term Memory, Capability-Search, MCP |

```bash
curl http://localhost:8095/examples      # Liste aller Cases (maschinenlesbar)
```

## 1 — Support-Triage (Structured Output + Memory + RAG + HITL)

Klassifiziert ein Ticket in ein typisiertes `TicketAssessment` (Kategorie/Priorität/Sentiment/
Antwortvorschlag), reichert es mit Kunden-Memory und einem Capability/RAG-Lookup an und pausiert
bei **HIGH**-Priorität durabel für eine menschliche Eskalations-Entscheidung.

```bash
curl -X POST "http://localhost:8095/examples/support-triage?customerId=c-1&message=Meine%20Rechnung%20ist%20falsch"
# falls HIGH: durabler Wartezustand
curl -X POST "http://localhost:8095/examples/support-triage/<workflowId>/escalate?approved=true"
```

Deterministische Regel: `TriagePolicy.requiresEscalation(json)` (getestet).

## 2 — Contract-Review (PII + Guardrail + HITL)

Der Agent prüft den Vertragstext; E-Mails/Telefon/Kreditkarten werden vor dem Modell maskiert
(`PiiAnonymizerHook`), Prompt-Injection wird blockiert (`Guardrail`). Danach wartet der Workflow
durabel auf die Freigabe.

```bash
curl -X POST "http://localhost:8095/examples/contract-review?contract=<text>"
curl -X POST "http://localhost:8095/examples/contract-review/<workflowId>/approve?approved=false"
```

## 3 — Invoice-Processing (durable Saga)

Vorwärts: `reserveBudget → postLedger → schedulePayment`. Schlägt die Zahlung fehl
(Betrag > `InvoicePolicy.DUAL_APPROVAL_THRESHOLD` = 100), kompensiert der Workflow in
umgekehrter Reihenfolge: `reverseLedger → releaseBudget`. Kompensation ist deterministischer
Workflow-Code und überlebt Worker-Neustarts.

```bash
curl -X POST "http://localhost:8095/examples/invoice?invoiceId=inv-1&amount=500"   # → compensated
curl -X POST "http://localhost:8095/examples/invoice?invoiceId=inv-2&amount=50"    # → paid
```

Getestet mit `TestWorkflowEnvironment`: `InvoiceWorkflowTest` prüft die exakte
Aufruf-Reihenfolge inkl. Kompensation.

## 4 — Market-Research (Sub-Agenten)

Ein Orchestrator-Agent delegiert die Recherche an einen `researcher`-Sub-Agenten (Web-Tools),
ein zweiter Agent schreibt daraus ein Executive Brief.

```bash
curl -X POST "http://localhost:8095/examples/market-research?topic=Temporal%20vs%20Kafka"
```

## 5 — Onboarding (Memory + Capability-Search + MCP)

Erinnert Projekt-Kontext (Memory), erzeugt einen persönlichen Einarbeitungsplan und kann Tools
eines externen MCP-Servers (SSE) auflisten.

```bash
curl -X POST "http://localhost:8095/examples/onboarding?newHire=Ada&projectScope=agentflow"
```

## Struktur

```
examples/
├── support/     (TicketAssessment, TriagePolicy, SupportActivities[+Impl], SupportTriageWorkflow)
├── contract/    (ContractReview, ContractActivities[+Impl], ContractReviewWorkflow)
├── invoice/     (ParsedInvoice, InvoicePolicy, InvoiceActivities[+Impl], InvoiceWorkflow)
├── research/    (ResearchActivities[+Impl], MarketResearchWorkflow)
├── onboarding/  (OnboardingPolicy, OnboardingActivities[+Impl], OnboardingWorkflow)
├── web/         (ExamplesController — REST-Einstieg)
├── BusinessCases.java  (Katalog der Cases)
└── Examples.java       (gemeinsame Agent-/Hook-Helfer)
```

## Deklarative Definitionen (quad.workflow v2)

Dieselben fünf Cases existieren **zusätzlich** als deklarative v2-Dokumente unter
`agentflow-app/src/main/resources/workflows/*.json` (jeweils mit eingebettetem `agents[]`).
Sie werden beim Start mit `quad-workflow-core` validiert und sind über
`GET /agentflow/workflows` sichtbar; ausführen mit
`POST /agentflow/runs/declarative?workflowId=<id>&task=<...>`.
Siehe `../README.md` → „Declarative workflow definitions".

## Tests

```bash
cd agentflow && mvn -pl agentflow-examples -am test
```

- `TriagePolicyTest`, `InvoicePolicyTest`, `BusinessCasesTest` — deterministische Regeln.
- `InvoiceWorkflowTest`, `SupportTriageWorkflowTest` — durable Workflows via
  `TestWorkflowEnvironment` mit handgeschriebenen Activity-Doubles (u. a. Kompensations- und
  Signal-Pfad).
