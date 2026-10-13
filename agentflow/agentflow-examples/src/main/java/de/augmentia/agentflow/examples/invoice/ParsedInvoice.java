package de.augmentia.agentflow.examples.invoice;

/** Structured output when an invoice is extracted from free text. */
public record ParsedInvoice(String invoiceId, double total, String currency, String vendor) {
}
