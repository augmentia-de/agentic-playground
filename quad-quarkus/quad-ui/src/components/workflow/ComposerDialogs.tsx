import { JsonSchemaPopup } from '../JsonSchemaPopup'
import type { EdgeInputMapping, WorkflowEdge, WorkflowNode } from '../../services/types'
import { EdgeInputsPopup } from './EdgeInputsPopup'

export interface ComposerDialogsProps {
  /** JSON output schema editor for the selected agent node. */
  schemaEditorOpen: boolean
  schemaNode: WorkflowNode | null
  onSchemaSave: (schema: string) => void
  onSchemaClose: () => void
  /** Input-source editor for the edge being edited (null = closed). */
  editEdgeInputId: string | null
  nodes: WorkflowNode[]
  edges: WorkflowEdge[]
  onEdgeInputsSave: (edgeId: string, inputs: EdgeInputMapping[]) => void
  onEdgeInputsClose: () => void
}

/**
 * The modal dialogs of the workflow composer (JSON schema editor + edge input sources). Extracted
 * from `WorkflowComposer.tsx` (WP12).
 */
export function ComposerDialogs({
  schemaEditorOpen,
  schemaNode,
  onSchemaSave,
  onSchemaClose,
  editEdgeInputId,
  nodes,
  edges,
  onEdgeInputsSave,
  onEdgeInputsClose,
}: ComposerDialogsProps) {
  return (
    <>
      {schemaEditorOpen && schemaNode?.type === 'agent' && (
        <JsonSchemaPopup
          initialSchema={(schemaNode.config?.jsonOutputSchema as string) ?? ''}
          title={`JSON Output Schema — ${schemaNode.title}`}
          onSave={onSchemaSave}
          onClose={onSchemaClose}
        />
      )}

      {editEdgeInputId && (() => {
        const edge = edges.find(e => e.id === editEdgeInputId)
        if (!edge) return null
        const tgt = nodes.find(n => n.id === edge.target)
        const initialInputs: EdgeInputMapping[] = edge.inputs ?? (edge.input ? [edge.input] : [{
          sourceNodeId: edge.source, format: 'text',
        }])
        const popupNodes = nodes.map(n => ({
          id: n.id, title: n.title, type: n.type,
          schema: (n.config?.jsonOutputSchema as string) ?? '',
        }))
        return (
          <EdgeInputsPopup
            nodes={popupNodes}
            targetTitle={tgt?.title ?? edge.target}
            initialInputs={initialInputs}
            onSave={inputs => onEdgeInputsSave(edge.id, inputs)}
            onClose={onEdgeInputsClose}
          />
        )
      })()}
    </>
  )
}
