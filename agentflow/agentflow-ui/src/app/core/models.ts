// Shared API models (kept permissive where the backend returns free-form JSON).

export interface Health {
  status?: string;
  components?: Record<string, { status?: string }>;
  [k: string]: unknown;
}

export interface RunSummary {
  workflowId: string;
  runId?: string;
  workflowType?: string;
  taskQueue?: string;
  status?: string;
  startTime?: string | null;
  closeTime?: string | null;
  historyLength?: number;
}

export interface RunDetail extends RunSummary {
  result?: string | null;
}

export interface RunEvent {
  eventId: number;
  type: string;
  time?: string | null;
  detail?: string;
}

export interface WorkflowDefinitionSummary {
  id: string;
  name: string;
  version: string;
  status: string;
  nodeCount: number;
  agentRefs: string[];
}

export type WorkflowDoc = Record<string, unknown>;

export interface ValidateResult {
  valid: boolean;
  summary?: { id: string; name: string; nodeCount: number };
  errors?: string[];
}

export interface Capability {
  name: string;
  description?: string;
  methodRef?: string;
  source?: string;
  type?: string;
}

export interface ExampleCase {
  id: string;
  title: string;
  endpoint: string;
  capabilities: string;
}

export interface Settings {
  llm: { model: string; baseUrl: string; hasApiKey: boolean; headersConfigured?: boolean; headerNames?: string[] };
  temporal: { target: string; namespace: string; taskQueue: string };
  capability: { index: string; table: string; autoload: boolean };
  workflows: { location: string; stateDir?: string };
  checkpoint: { path: string };
  endpoints: { metrics: string; health: string; temporalUi: string };
}

export interface StartEngineField {
  name: string;
  label: string;
  type: 'text' | 'number' | 'textarea' | 'select';
  required?: boolean;
  defaultValue?: string | number;
  options?: string[];
  /** Populates a select from a REST source instead of the static {@link options}. */
  optionsFrom?: 'declarative-workflows';
  placeholder?: string;
}

export interface StartEngine {
  key: string;
  label: string;
  path: string;
  fields: StartEngineField[];
  hint?: string;
}
