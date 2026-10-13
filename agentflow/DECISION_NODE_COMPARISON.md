# `decision` node — comparison quad-quarkus · kafka-platform · agentflow

**Status:** Comparison (2026-10-10). **No change** to the agentflow `decision` implementation —
this document only describes the semantics of the three engines.

## 1. Canonical form (v2 schema, shared)

The `quad.workflow` v2 schema (`quad-workflow-core`) defines `nodeDecision`:

```json
{
  "id": "pick", "type": "decision", "dependsOn": ["start"],
  "branches": [ { "branchId": "fast", "condition": "contains:fast", "target": "fastPath" } ],
  "defaultTarget": "slowPath"
}
```

- **agentflow** uses exactly this form (`NodeSpec.branches` + `defaultTarget`).
- **kafka** uses the same form (`DagNode.branches` + `defaultTarget`), only with `stepId`/`nodeType "DECISION"`.
- **quad** has the form in the schema **and** in `ConditionalNode`; the *engine*, by contrast, uses a
  boolean pair.

## 2. Runtime semantics compared

| Aspect | **agentflow** | **quad-quarkus** | **kafka-platform** |
|---|---|---|---|
| Node form (schema) | `branches[]` + `defaultTarget` | `branches[]` + `defaultTarget` (schema) | `branches[]` + `defaultTarget` |
| Runtime execution | `DeclarativeAgentWorkflow`: first matching branch, otherwise `defaultTarget`; unselected targets are **pruned** | `ConditionalNodeExecutor`: **one** condition → `trueTarget`/`falseTarget`; unreachable downstream nodes are marked as "skipped by conditional" | `RoutingEvaluator`: branches in order, first truthy wins, otherwise `defaultTarget` |
| Persisted decision | visible in the workflow result/plan (branch target as the node's output) | `run` state + node outputs | `Decision(stepId, branchId, chosenTarget, iteration)`, `branchId="default"` for the default |
| Condition language | `ConditionEvaluator`: `contains:`, `notcontains:`, `starts:`, `equals:`, `truthy`, `falsy` (empty = true) | `WorkflowCondition.matches`: `contains:`, `notcontains:`, `matches:` (regex), `iter<op><n>`; separately `ExpressionExitCondition` `key >= value` (scores, `AgentScope`) | `ExpressionEvaluator`: **full expression parser** over `context`/`input` maps, e.g. `context['research_step'].confidence < 0.8` (paths, numbers, strings, bool, comparisons/logic) |
| Branch model | n branches + default | 1 condition → 2 targets (+ schema branches) | n branches + default (+ `guardCondition` per node) |
| Unselected paths | pruned → successors are **skipped** | downstream "skipped by conditional" | not fired (routing), checkpoint remembers `decision_branch` |
| Pruning rule | `join` with `any`/`n_of_m` stays runnable; `end` never skipped | reachability from `true/falseTarget` | routing `fireableSteps` purely from persisted state |

## 3. Expressiveness (brief conclusion)

```
Expressiveness:      kafka  >  quad (WorkflowCondition)  >  agentflow
Structural equality: agentflow = quad(schema) = kafka   (branches[] + defaultTarget)
```

- **kafka** is the most powerful (a real expression parser over node outputs), but also the most complex.
- **quad** is split in two: schema `decision` (like agentflow/kafka) vs. engine `ConditionalNode`
  (boolean) plus `WorkflowCondition` (contains/notcontains/matches/iter).
- **agentflow** is deliberately minimal (a small, quickly testable condition language) and therefore
  the easiest to keep deterministic.

## 4. Consequence / non-change

Deliberately **no change** to agentflow `decision` in this step. If alignment is desired later,
optional, backward-compatible extensions of `ConditionEvaluator` would be conceivable:

1. `matches:<regex>` (quad parity) — small, without a semantic break.
2. `context.<node>.<field> <op> <value>` (kafka parity) — a bigger step (path resolution over
   node outputs, parser instead of prefix matching).

Both would leave the existing branches/`defaultTarget` formats unchanged.
