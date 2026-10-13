# `decision`-Knoten — Vergleich quad-quarkus · kafka-platform · agentflow

**Status:** Vergleich (2026-10-10). **Keine Änderung** an der agentflow-`decision`-Implementierung —
dieses Dokument beschreibt nur die Semantik der drei Engines.

## 1. Kanonische Form (v2-Schema, geteilt)

Das `quad.workflow`-v2-Schema (`quad-workflow-core`) definiert `nodeDecision`:

```json
{
  "id": "pick", "type": "decision", "dependsOn": ["start"],
  "branches": [ { "branchId": "fast", "condition": "contains:fast", "target": "fastPath" } ],
  "defaultTarget": "slowPath"
}
```

- **agentflow** nutzt genau diese Form (`NodeSpec.branches` + `defaultTarget`).
- **kafka** nutzt die gleiche Form (`DagNode.branches` + `defaultTarget`), nur mit `stepId`/`nodeType "DECISION"`.
- **quad** hat die Form im Schema **und** in `ConditionalNode`; die *Engine* nutzt davon abweichend ein
  boolesches Paar.

## 2. Laufzeit-Semantik im Vergleich

| Aspekt | **agentflow** | **quad-quarkus** | **kafka-platform** |
|---|---|---|---|
| Knotenform (Schema) | `branches[]` + `defaultTarget` | `branches[]` + `defaultTarget` (Schema) | `branches[]` + `defaultTarget` |
| Laufzeit-Ausführung | `DeclarativeAgentWorkflow`: erster passender Branch, sonst `defaultTarget`; nicht gewählte Targets werden **gepruned** | `ConditionalNodeExecutor`: **eine** Bedingung → `trueTarget`/`falseTarget`; nicht erreichbare Downstream-Nodes werden als „skipped by conditional“ markiert | `RoutingEvaluator`: Branches in Reihenfolge, erster truthy gewinnt, sonst `defaultTarget` |
| Persistierte Entscheidung | im Workflow-Ergebnis/Plan sichtbar (Branch-Target als Output des Knotens) | `run`-State + Node-Outputs | `Decision(stepId, branchId, chosenTarget, iteration)`, `branchId="default"` bei Default |
| Condition-Sprache | `ConditionEvaluator`: `contains:`, `notcontains:`, `starts:`, `equals:`, `truthy`, `falsy` (leer = true) | `WorkflowCondition.matches`: `contains:`, `notcontains:`, `matches:` (Regex), `iter<op><n>`; separat `ExpressionExitCondition` `key >= value` (Scores, `AgentScope`) | `ExpressionEvaluator`: **vollständiger Ausdrucksparser** über `context`/`input`-Maps, z. B. `context['research_step'].confidence < 0.8` (Pfade, Zahlen, Strings, Bool, Vergleiche/Logik) |
| Branch-Modell | n Branches + Default | 1 Condition → 2 Ziele (+ Schema-Branches) | n Branches + Default (+ `guardCondition` je Node) |
| Nicht gewählte Pfade | gepruned → Nachfolger werden **skipped** | Downstream „skipped by conditional“ | nicht gefeuert (Routing), Checkpoint merkt sich `decision_branch` |
| Pruning-Regel | `join` mit `any`/`n_of_m` bleibt lauffähig; `end` nie geskippt | Erreichbarkeit ab `true/falseTarget` | Routing-`fireableSteps` rein aus persistiertem Zustand |

## 3. Ausdrucksstärke (Kurzfazit)

```
Ausdrucksstärke:   kafka  >  quad (WorkflowCondition)  >  agentflow
Struktur-Gleichheit: agentflow = quad(Schema) = kafka   (branches[] + defaultTarget)
```

- **kafka** ist am mächtigsten (echter Ausdrucksparser über Knoten-Outputs), dafür am komplexesten.
- **quad** ist zweigeteilt: Schema-`decision` (wie agentflow/kafka) vs. Engine-`ConditionalNode`
  (boolesch) plus `WorkflowCondition` (contains/notcontains/matches/iter).
- **agentflow** ist bewusst minimal (kleine, schnell testbare Condition-Sprache) und damit am
  leichtesten deterministisch zu halten.

## 4. Konsequenz / Nicht-Änderung

Bewusst **keine Änderung** an agentflow-`decision` in diesem Schritt. Wenn später Angleichung
gewünscht ist, wären optionale, abwärtskompatible Erweiterungen von `ConditionEvaluator` denkbar:

1. `matches:<regex>` (quad-Parität) — klein, ohne Semantikbruch.
2. `context.<node>.<field> <op> <value>` (kafka-Parität) — größerer Schritt (Pfadauflösung über
   Knoten-Outputs, Parser statt Präfix-Matching).

Beides würde die bestehenden Branches/`defaultTarget`-Formate unverändert lassen.
