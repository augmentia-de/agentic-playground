# WP05 — quad-core: Guardrails, PII, Observability

| | |
|---|---|
| **Status** | offen |
| **Schwere** | Mittel |
| **Module** | `quad-quarkus/quad-core` |
| **Review-Referenz** | B17, B18, B19, B20, B22 (§5.3, §5.2) |
| **Abhängigkeit** | – |
| **Geschätzter Aufwand** | 2 Tage |

## Ziel

Guardrails wirken wie dokumentiert, PII-Maskierung mutiert keinen geteilten Zustand, Tracing
enthält die Tool-Identität, und die Hook-Pipeline verschluckt keine Fehler mehr.

## Befunde (Ist-Zustand)

- **B17 – Guardrail-Aktionen ignoriert:** `GuardrailPlugin` behandelt `THROW`/`ESCALATE` nicht;
  alles degradiert zu `Cancel` (`GuardrailPlugin.java:99-104`).
- **B18 – Live-Liste mutiert:** `PiiAnonymizerHook` schreibt via `messages.set(...)` in die laufende
  Liste (`PiiAnonymizerHook.java:64`), anders als der nicht-mutierende `PiiRedactionHook`.
- **B19 – PII-Regex case-sensitiv:** `AgentBuilder.java:453-456` nutzt `[A-Z0-9…]` → Kleinbuchstaben
  (E-Mails!) passieren ungefiltert.
- **B20 – Tool-Name verworfen:** `OTelAgentTracer.addEvent` ignoriert `toolName` (`:74-79`).
- **B22 – Exceptions verschluckt:** `HookRegistry.wrap` schluckt Exceptions; `failurePolicy` nicht
  `volatile` (`:163-172`).

## 1. Tests zuerst (Red)

Testort: `quad-quarkus/quad-core/src/test/java/de/augmentia/quad/core/…`
(vorhanden: `guardrails/GuardrailPluginTest`, `agent/AgentBuilderTest`, `hook/…`, `observability/…`).

| # | Test | Erwartung |
|---|------|-----------|
| 1 | `GuardrailPluginTest#throwAction_throws` | `THROW` → Exception propagiert |
| 2 | `GuardrailPluginTest#escalateAction_escalates` | `ESCALATE` → Eskalationspfad/HITL |
| 3 | `GuardrailPluginTest#cancelAction_cancels` | `Cancel` bleibt wie bisher |
| 4 | `PiiAnonymizerHookTest#doesNotMutateOriginalList` | Original unverändert, nur Ergebnis maskiert |
| 5 | `PiiAnonymizerHookTest#masksLowercaseEmail` | `user@example.com` wird maskiert |
| 6 | `AgentBuilderTest#piiPatternCaseInsensitive` | Klein-/Großschreibung gleich behandelt |
| 7 | `OtelTracerTest#addEvent_includesToolName` | Span-Event enthält Tool-Name |
| 8 | `HookRegistryTest#wrap_propagatesOrLogsPerPolicy` | je `failurePolicy` (THROW/LOG) korrekt |
| 9 | `HookRegistryTest#failurePolicyVisibleAcrossThreads` | `volatile`/immutable → Sichtbarkeit |

**Abnahme (Red):** 1, 2, 4, 5, 7, 8 fehlschlagen.

## 2. Änderungen (Green)

1. **`GuardrailPlugin`:** vollständige Aktionsbehandlung (`THROW`, `ESCALATE`, `Cancel`, ggf. `RETRY`);
   unbekannte Aktion → `IllegalArgumentException` statt stiller `Cancel`.
2. **`PiiAnonymizerHook`:** kopieren statt mutieren (gleiche Semantik wie `PiiRedactionHook`); ggf.
   beide Hooks zusammenführen/klare Doku, welcher wann greift.
3. **PII-Pattern:** `CASE_INSENSITIVE` (`(?i)`) bzw. Zeichenklassen `[A-Za-z0-9]`.
4. **`OTelAgentTracer`:** `toolName` als Attribut/Event-Feld nutzen.
5. **`HookRegistry`:** `failurePolicy` `volatile` oder immutable Feld; `wrap` loggt mit Kontext und
   respektiert Policy (kein stilles Verschlucken).

## 3. Verifikation

```bash
cd quad-quarkus && mvn -pl quad-core test
cd quad-quarkus && mvn -pl quad-core test -Dtest='Guardrail*,Pii*,*Tracer*,HookRegistry*'
```

## 4. Risiken / Hinweise

- Änderung an Guardrail-Semantik kann bestehende Workflows beeinflussen → Verhalten pro Aktion in
  Doku festhalten.
- `PiiAnonymizerHook` vs. `PiiRedactionHook`: Doppelkonzept klären (ggf. in WP13 zusammenführen).

## Definition of Done

- [ ] Tests 1–9 grün.
- [ ] Guardrail-Aktionen wirken wie dokumentiert.
- [ ] PII maskiert case-insensitiv und ohne Seiteneffekte.
- [ ] Tool-Name im Tracing vorhanden.
- [ ] Hook-Fehler werden policy-konform behandelt.
- [ ] Fortschrittslog gepflegt.

## Fortschrittslog

| Datum | Bearbeiter | Schritt | Ergebnis |
|-------|-----------|---------|----------|
| | | 1. Tests rot | |
| | | 2. Umsetzung | |
| | | 3. Verifikation | |
