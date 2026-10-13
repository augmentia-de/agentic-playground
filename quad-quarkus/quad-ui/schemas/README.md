# QUAD UI – API Definitions

This folder contains the **JSON schema contracts** for all data that is exchanged between QUAD UI
and the QUAD backend. The TypeScript types in
`src/services/types.ts` are the 1:1 translation of these schemas.

## Schemas

| File | Content | Backend endpoints |
|-------|--------|-------------------|
| `agent.schema.json` | Agent definition (request/response) | `/api/ui/agents` (GET/POST/PUT/DELETE) |
| `workflow.schema.json` | Workflow definition incl. nodes/edges | `/api/ui/workflows` (GET/POST) |
| `execute.schema.json` | Task and workflow execution | `/api/ui/execute`, `/api/ui/workflows/{id}/execute` |
| `monitor.schema.json` | Metrics, audit, HITL | `/api/ui/metrics`, `/audit/logs`, `/hitl/approvals` |

## Validation (Example)

```bash
npx ajv-cli validate -s schemas/workflow.schema.json -d beispiele/workflow.json
```

## Workflow Data Flow

```
[Node n1: research] ──edge e1──▶ [Node n2: review]
        │                                  ▲
        └── Output becomes context ────────┘
             ("Input from upstream node")
```

- The execution order is determined by the **directed edges**
  (topological sorting in the backend).
- Each node produces an LLM output (`output` in `WorkflowNodeResult`).
- Outputs of upstream nodes are added to the prompt of downstream nodes as
  `Input from upstream node: <output>`.
- Nodes without predecessors are executed first.

## Example: Mini Workflow

```json
{
  "name": "Mini Research Workflow",
  "nodes": [
    { "id": "n1", "type": "research", "title": "What is 2+2?", "status": "pending" },
    { "id": "n2", "type": "review", "title": "Verify the previous answer", "status": "pending" }
  ],
  "edges": [
    { "id": "e1", "source": "n1", "target": "n2" }
  ]
}
```

POST to `/api/ui/workflows` → `{ "id": "workflow-..." }` → then
`POST /api/ui/workflows/{id}/execute`.
