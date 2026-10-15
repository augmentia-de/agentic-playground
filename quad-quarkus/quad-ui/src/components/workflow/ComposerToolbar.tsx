import type { AgentDefinition, WorkflowNodeType } from '../../services/types'
import * as kafkaApi from '../../services/kafkaApi'

export interface ComposerToolbarProps {
  agents: AgentDefinition[]
  kafkaWorkflows: kafkaApi.KafkaWorkflowDefinition[]
  backend: 'quarkus' | 'kafka'
  nodeCount: number
  isSaved: boolean
  saving: boolean
  running: boolean
  materializing: boolean
  insertAgentId: string
  selectedNodeId: string | null
  onBackendChange: (backend: 'quarkus' | 'kafka') => void
  onInsertAgentIdChange: (id: string) => void
  onAddNode: (type: WorkflowNodeType, agentId?: string, agentName?: string, afterId?: string) => void
  onNewWorkflow: () => void
  onReset: () => void
  onSave: () => void
  onExecute: () => void
  onMaterialize: () => void
  onDematerialize: () => void
}

export function ComposerToolbar({
  agents,
  kafkaWorkflows,
  backend,
  nodeCount,
  isSaved,
  saving,
  running,
  materializing,
  insertAgentId,
  selectedNodeId,
  onBackendChange,
  onInsertAgentIdChange,
  onAddNode,
  onNewWorkflow,
  onReset,
  onSave,
  onExecute,
  onMaterialize,
  onDematerialize,
}: ComposerToolbarProps) {
  return (
      <div className="toolbar" role="toolbar">
        <button type="button" onClick={onNewWorkflow}>+ New</button>
        <span className="toolbar-separator" />
        <select
          value={insertAgentId}
          onChange={e => onInsertAgentIdChange(e.target.value)}
          aria-label="Select agent to insert"
        >
          <option value="">-- select agent --</option>
          {agents.map(a => (
            <option key={a.id} value={a.id}>{a.name} ({a.category})</option>
          ))}
        </select>
        <button
          type="button"
          onClick={() => {
            if (!insertAgentId) return
            const agent = agents.find(a => a.id === insertAgentId)
            onAddNode('agent', insertAgentId, agent?.name ?? insertAgentId, selectedNodeId ?? undefined)
          }}
          disabled={!insertAgentId}
        >
          + Insert
        </button>
        <span className="toolbar-separator" />
        <button type="button" className="messaging" onClick={() => onAddNode('messaging-in')}>📥 Messaging In</button>
        <button type="button" className="messaging" onClick={() => onAddNode('messaging-out', undefined, undefined, selectedNodeId ?? undefined)}>📤 Messaging Out</button>
        <span className="toolbar-separator" />
        <button type="button" className="control-flow" onClick={() => onAddNode('loop', undefined, undefined, selectedNodeId ?? undefined)}>⟳ Loop</button>
        <button type="button" className="control-flow" onClick={() => onAddNode('conditional', undefined, undefined, selectedNodeId ?? undefined)}>⤳ Conditional</button>
        <select
          className="more-nodes"
          aria-label="Add more nodes"
          value=""
          onChange={e => {
            const t = e.target.value
            if (t) { onAddNode(t as WorkflowNodeType, undefined, undefined, selectedNodeId ?? undefined); e.target.value = '' }
          }}
        >
          <option value="">+ Weitere Knoten…</option>
          <option value="nested-workflow">⧉ Nested Workflow</option>
          <option value="async">⚡ Async</option>
          <option value="fork">⎀ Fork</option>
          <option value="join">⧉ Join</option>
        </select>
        <span className="toolbar-separator" />
        <button type="button" onClick={onReset} disabled={nodeCount === 0}>Reset</button>
        <span className="toolbar-separator" />
        <select
          value={backend}
          onChange={e => onBackendChange(e.target.value as 'quarkus' | 'kafka')}
          aria-label="Execution backend"
          style={{ minWidth: '12em' }}
        >
          <option value="quarkus">🟣 Quarkus Backend</option>
          <option value="kafka">🟢 Kafka Orchestrator</option>
        </select>
        {backend === 'kafka' && (
          <select
            aria-label="Kafka workflow definitions"
            title="Workflow definitions already persisted in the Kafka orchestrator"
            value=""
            onChange={() => undefined}
            style={{ minWidth: '12em' }}
          >
            <option value="">
              {kafkaWorkflows.length === 0
                ? 'No Kafka workflows'
                : `${kafkaWorkflows.length} Kafka workflow${kafkaWorkflows.length === 1 ? '' : 's'}`}
            </option>
            {kafkaWorkflows.map(w => (
              <option key={w.id} value={w.id} disabled>
                {w.name} v{w.version}{w.isActive ? ' (active)' : ''}
              </option>
            ))}
          </select>
        )}
        <span className="toolbar-separator" />
        <button type="button" onClick={onSave} disabled={nodeCount === 0 || saving}>
          {saving ? 'Saving...' : isSaved ? 'Save (Update)' : 'Save'}
        </button>
        <button type="button" onClick={onExecute} disabled={nodeCount === 0 || running}>
          {running ? 'Running...' : 'Execute'}
        </button>
        <button
          type="button"
          onClick={onMaterialize}
          disabled={!isSaved || materializing}
          title="Aktiviert die internen Step-Agenten (active=false), damit sie im Agents-Tab sichtbar und bearbeitbar sind."
        >
          {materializing ? 'Activating…' : 'Agents editierbar machen'}
        </button>
        <button
          type="button"
          onClick={onDematerialize}
          disabled={!isSaved || materializing}
          title="Verbirgt die internen Step-Agenten wieder (active=false)."
        >
          Agents verbergen
        </button>
      </div>
  )
}
