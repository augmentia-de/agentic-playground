import type { EdgeInputMapping, WorkflowEdge, WorkflowNode } from '../../services/types'
import { INITIAL_DATA_FIELD } from './graphUtils'

export interface EdgeEditorProps {
  nodes: WorkflowNode[]
  edges: WorkflowEdge[]
  onUpdateEdge: (id: string, updates: Partial<WorkflowEdge>) => void
  onRemoveEdge: (id: string) => void
  onEditInputs: (edgeId: string) => void
}

export function EdgeEditor({ nodes, edges, onUpdateEdge, onRemoveEdge, onEditInputs }: EdgeEditorProps) {
  return (
        <section className="edge-editor panel">
          <h3>Data Flow Between Steps</h3>
          {edges.length === 0 ? (
            <p className="empty-state">No edges yet. New nodes are auto-connected.</p>
          ) : (
            <table>
              <thead>
                <tr><th>From</th><th>To</th><th>Input Sources</th><th>Join</th><th></th></tr>
              </thead>
              <tbody>
                {edges.map(edge => {
                  const tgt = nodes.find(n => n.id === edge.target)
                  const srcTitle = (id: string) =>
                    nodes.find(n => n.id === id)?.title
                    ?? (id === INITIAL_DATA_FIELD.value ? INITIAL_DATA_FIELD.label : id)
                  const inputs: EdgeInputMapping[] = edge.inputs ?? (edge.input ? [edge.input] : [{
                    sourceNodeId: edge.source, format: 'text',
                  }])
                  const isCustom = !!edge.inputs || !!edge.input
                  const preview = inputs.length === 0
                    ? '—'
                    : inputs.map(src => srcTitle(src.sourceNodeId))
                        .map((t, i) => (i > 0 ? ' + ' : '') + t)
                        .join('')
                  return (
                    <tr key={edge.id}>
                      <td>{nodes.find(n => n.id === edge.source)?.title ?? edge.source}</td>
                      <td>{tgt?.title ?? edge.target}</td>
                      <td>
                        <button
                          type="button"
                          className="source-button"
                          onClick={() => onEditInputs(edge.id)}
                          title={preview}
                          aria-label={`Edit input sources for ${tgt?.title ?? ''}`}
                        >
                          edit ({isCustom ? 'custom' : 'default'})
                          {isCustom && <span className="source-count">{inputs.length}</span>}
                        </button>
                      </td>
                      <td>
                        <select
                          value={edge.joinPolicy ?? 'all'}
                          onChange={e => onUpdateEdge(edge.id, { joinPolicy: e.target.value as 'all' | 'any' })}
                          aria-label={`Join policy for ${tgt?.title ?? ''}`}
                        >
                          <option value="all">all</option>
                          <option value="any">any</option>
                        </select>
                      </td>
                      <td>
                        <button type="button" className="danger" onClick={() => onRemoveEdge(edge.id)}>✕</button>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          )}
        </section>
  )
}
