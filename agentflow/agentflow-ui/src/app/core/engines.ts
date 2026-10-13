import { StartEngine } from './models';

/** All startable engines/workflows exposed by agentflow-app (drives the Runs page forms). */
export const ENGINES: StartEngine[] = [
  {
    key: 'graph',
    label: 'LangGraph4j (plan → execute → review)',
    path: '/agentflow/runs',
    fields: [{ name: 'input', label: 'Input', type: 'textarea', required: true }],
  },
  {
    key: 'quad',
    label: 'quad agent (Agents.simple)',
    path: '/agentflow/runs/quad',
    fields: [{ name: 'input', label: 'Input', type: 'textarea', required: true }],
  },
  {
    key: 'quad-rich',
    label: 'Rich quad agent (tools/skills/PII/audit)',
    path: '/agentflow/runs/quad-rich',
    fields: [
      { name: 'input', label: 'Input', type: 'textarea', required: true },
      { name: 'tools', label: 'Tools (comma, empty = all)', type: 'text', placeholder: 'readFile,listDirectory' },
      { name: 'skillsDir', label: 'Skills dir', type: 'text', placeholder: './skills' },
    ],
  },
  {
    key: 'authorized',
    label: 'RBAC agent (role-scoped tools)',
    path: '/agentflow/runs/authorized',
    fields: [
      { name: 'input', label: 'Input', type: 'textarea', required: true },
      { name: 'roles', label: 'Roles (comma)', type: 'text', placeholder: 'reader,researcher,writer' },
    ],
  },
  {
    key: 'hitl',
    label: 'HITL (graph pauses for approval)',
    path: '/agentflow/runs/hitl',
    fields: [{ name: 'input', label: 'Input', type: 'textarea', required: true }],
    hint: 'Nach dem Start über die HITL-Seite approve/reject senden.',
  },
  {
    key: 'structured',
    label: 'Structured Output (ContentPlan)',
    path: '/agentflow/runs/structured',
    fields: [{ name: 'input', label: 'Input', type: 'textarea', required: true }],
  },
  {
    key: 'provenance',
    label: 'Provenance (tool origins)',
    path: '/agentflow/runs/provenance',
    fields: [{ name: 'input', label: 'Input', type: 'textarea', required: true }],
  },
  {
    key: 'subagents',
    label: 'Sub-Agents (orchestrator + researcher)',
    path: '/agentflow/runs/subagents',
    fields: [{ name: 'input', label: 'Input', type: 'textarea', required: true }],
  },
  {
    key: 'metrics',
    label: 'Metrics/OTel run',
    path: '/agentflow/runs/metrics',
    fields: [{ name: 'input', label: 'Input', type: 'textarea', required: true }],
  },
  {
    key: 'capability-search',
    label: 'Capability-Search / RAG',
    path: '/agentflow/runs/capability-search',
    fields: [
      { name: 'input', label: 'Input', type: 'textarea', required: true },
      { name: 'query', label: 'Query (default = input)', type: 'text' },
      { name: 'topK', label: 'Top K', type: 'number', defaultValue: 10 },
    ],
  },
  {
    key: 'memory',
    label: 'Memory (remember / recall)',
    path: '/agentflow/runs/memory',
    fields: [
      { name: 'op', label: 'Op', type: 'select', options: ['remember', 'recall'], defaultValue: 'remember' },
      { name: 'scope', label: 'Scope', type: 'text', required: true, placeholder: 'project-a' },
      { name: 'content', label: 'Content (remember)', type: 'textarea' },
    ],
  },
  {
    key: 'mcp',
    label: 'MCP (list / run tool)',
    path: '/agentflow/runs/mcp',
    fields: [
      { name: 'op', label: 'Op', type: 'select', options: ['list', 'run'], defaultValue: 'list' },
      { name: 'serverUrl', label: 'Server URL (SSE)', type: 'text', placeholder: 'http://localhost:3001/sse' },
      { name: 'tool', label: 'Tool (run)', type: 'text' },
      { name: 'args', label: 'Args (JSON)', type: 'textarea', defaultValue: '{}' },
    ],
  },
  {
    key: 'saga',
    label: 'Saga (reserve → charge, compensation)',
    path: '/agentflow/runs/saga',
    fields: [
      { name: 'orderId', label: 'Order ID', type: 'text', required: true, defaultValue: 'o-1' },
      { name: 'amount', label: 'Amount (>100 → compensation)', type: 'number', defaultValue: 50 },
    ],
  },
  {
    key: 'quad-saga',
    label: 'quad session saga (unwind)',
    path: '/agentflow/runs/quad-saga',
    fields: [{ name: 'input', label: 'Input', type: 'text', defaultValue: 'demo' }],
  },
  {
    key: 'declarative',
    label: 'Declarative workflow (v2 document)',
    path: '/agentflow/runs/declarative',
    fields: [
      { name: 'workflowId', label: 'Workflow', type: 'select', required: true, optionsFrom: 'declarative-workflows' },
      { name: 'task', label: 'Task', type: 'textarea', required: true },
    ],
    hint: 'Workflow aus der Liste wählen. Approval-Knoten über /agentflow/runs/declarative/{id}/approve.',
  },
];
