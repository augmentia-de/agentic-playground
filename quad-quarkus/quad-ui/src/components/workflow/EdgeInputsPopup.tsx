import { useState } from 'react'
import type { EdgeInputMapping } from '../../services/types'
import { INITIAL_DATA_FIELD, SESSION_FIELDS } from './graphUtils'
import { SchemaPathPicker } from './SchemaPathPicker'

interface SourceOption {
  value: string
  label: string
}

interface PopupSourceNode {
  id: string
  title: string
  type: string
  /** JSON-Schema des Agent-Outputs (node.config.jsonOutputSchema), optional. */
  schema?: string
}

export interface EdgeInputsPopupProps {
  nodes: PopupSourceNode[]
  targetTitle: string
  initialInputs: EdgeInputMapping[]
  onSave: (inputs: EdgeInputMapping[]) => void
  onClose: () => void
}

/**
 * Modal for editing the input sources of an edge. Allows a list of
 * arbitrary predecessor outputs: for 'text' only one source, for 'json' additionally
 * a subtree path from the source agent's JSON schema, selectable visually (tree)
 * or manually, whose content is passed to the following node as JSON.
 */
export function EdgeInputsPopup({ nodes, targetTitle, initialInputs, onSave, onClose }: EdgeInputsPopupProps) {
  const sourceOptions: SourceOption[] = [
    ...nodes.map(n => ({ value: n.id, label: n.title || n.id })),
    { value: INITIAL_DATA_FIELD.value, label: INITIAL_DATA_FIELD.label },
    ...SESSION_FIELDS.map(f => ({ value: f.value, label: f.label })),
  ]
  const [inputs, setInputs] = useState<EdgeInputMapping[]>(
    initialInputs.length > 0 ? initialInputs.map(i => ({ ...i, format: i.format ?? 'text' })) : []
  )
  const [draftSource, setDraftSource] = useState(sourceOptions[0]?.value ?? '')
  const [draftFormat, setDraftFormat] = useState<'text' | 'json'>('text')
  const [draftPath, setDraftPath] = useState('')
  const [draftPathManual, setDraftPathManual] = useState(false)
  const [openPicker, setOpenPicker] = useState<Record<string, boolean>>({})

  const schemaOf = (nodeId: string) => nodes.find(n => n.id === nodeId)?.schema ?? ''

  /** Pfadliste eines JSON-Inputs; migriert das einzelne `path`-Feld nach `paths`. */
  const pathsOf = (input: EdgeInputMapping): string[] => {
    if (input.paths && input.paths.length > 0) return input.paths
    if (input.path) return [input.path]
    return ['']
  }

  /** Writes back the path list (and maintains `path` as a shorthand for a single path). */
  const setPaths = (idx: number, paths: string[]) => {
    const filtered = paths.filter(p => p.trim() !== '')
    updateInput(idx, {
      paths: filtered.length > 0 ? filtered : undefined,
      path: filtered.length === 1 ? filtered[0] : undefined,
    })
  }

  const updatePath = (idx: number, pIdx: number, value: string) => {
    const paths = [...pathsOf(inputs[idx])]
    paths[pIdx] = value
    setPaths(idx, paths)
  }

  const addPath = (idx: number) => {
    setPaths(idx, [...pathsOf(inputs[idx]), ''])
  }

  const removePath = (idx: number, pIdx: number) => {
    setPaths(idx, pathsOf(inputs[idx]).filter((_, k) => k !== pIdx))
  }

  const pickPath = (idx: number, pIdx: number, dotPath: string) => {
    updatePath(idx, pIdx, dotPath)
    setOpenPicker({})
  }

  const togglePicker = (idx: number, pIdx: number) => {
    const key = `${idx}:${pIdx}`
    setOpenPicker(prev => ({ ...prev, [key]: !prev[key] }))
  }

  const isPickerOpen = (idx: number, pIdx: number) => !!openPicker[`${idx}:${pIdx}`]

  const addInput = () => {
    const src = draftSource
    if (!src) return
    const existing = inputs.find(i => i.sourceNodeId === src)
    if (existing) {
      if (existing.format === draftFormat) return
      setInputs(inputs.map(i =>
        i.sourceNodeId === src
          ? {
              ...i,
              format: draftFormat,
              path: draftFormat === 'json' ? (i.path ?? undefined) : undefined,
              paths: draftFormat === 'json' ? (i.paths ?? undefined) : undefined,
            }
          : i
      ))
      return
    }
    const newInput: EdgeInputMapping = { sourceNodeId: src, format: draftFormat }
    if (draftFormat === 'json' && draftPath.trim()) newInput.path = draftPath.trim()
    setInputs([...inputs, newInput])
    setDraftPath('')
  }

  const updateInput = (idx: number, patch: Partial<EdgeInputMapping>) => {
    setInputs(inputs.map((i, k) => (k === idx ? { ...i, ...patch } : i)))
  }

  const removeInput = (idx: number) => {
    setInputs(inputs.filter((_, k) => k !== idx))
  }

  const labelOf = (value: string) => sourceOptions.find(o => o.value === value)?.label ?? value

  return (
    <div className="agent-editor-overlay" role="dialog" aria-label={`Input sources for ${targetTitle}`}>
      <div className="agent-editor modal edge-inputs-popup">
        <h3>Input Sources — {targetTitle}</h3>
        <p className="schema-popup-hint">
          Choose arbitrary predecessor outputs as input. For <strong>JSON</strong>, the content
          of one or more subtree paths (via the tree view or as <code>items[0].name</code>)
          can be passed to the following node. Multiple sources are allowed.
        </p>

        <div className="edge-inputs-list">
          {inputs.length === 0 && <p className="empty-state">No sources added yet.</p>}
          {inputs.map((input, idx) => {
            const isNode = nodes.some(n => n.id === input.sourceNodeId)
            const isJson = (input.format ?? 'text') === 'json'
            return (
              <div className="edge-input-row" key={`${input.sourceNodeId}-${idx}`}>
                <div className="edge-input-row-head">
                  <span className="edge-input-name">{labelOf(input.sourceNodeId)}</span>
                  <select
                    value={input.format ?? 'text'}
                    onChange={e => updateInput(idx, {
                      format: e.target.value as 'text' | 'json',
                      path: e.target.value === 'json' ? input.path : undefined,
                    })}
                    aria-label={`Format for ${labelOf(input.sourceNodeId)}`}
                  >
                    <option value="text">Text</option>
                    <option value="json">JSON</option>
                  </select>
                  <button type="button" className="danger" onClick={() => removeInput(idx)}>✕</button>
                </div>
                {isJson && isNode && (
                  <div className="edge-input-path-block">
                    {pathsOf(input).map((pathVal, pIdx) => (
                      <div className="edge-input-path-item" key={`${idx}-${pIdx}`}>
                        <div className="edge-input-path-head">
                          <input
                            className="edge-input-path"
                            value={pathVal}
                            placeholder="Subtree path e.g. items[0].name"
                            onChange={e => updatePath(idx, pIdx, e.target.value)}
                            aria-label={`Subtree path ${pIdx + 1} for ${labelOf(input.sourceNodeId)}`}
                          />
                          {schemaOf(input.sourceNodeId) && (
                            <button
                              type="button"
                              className="schema-tree-open"
                              onClick={() => togglePicker(idx, pIdx)}
                              aria-label={`Toggle tree path selection for path ${pIdx + 1}`}
                            >
                              {isPickerOpen(idx, pIdx) ? 'Close' : 'Tree'}
                            </button>
                          )}
                          <button
                            type="button"
                            className="danger path-remove"
                            onClick={() => removePath(idx, pIdx)}
                            aria-label={`Remove path ${pIdx + 1}`}
                          >
                            ✕
                          </button>
                        </div>
                        {isPickerOpen(idx, pIdx) && schemaOf(input.sourceNodeId) && (
                          <SchemaPathPicker
                            schemaRaw={schemaOf(input.sourceNodeId)}
                            value={pathVal}
                            onSelect={dotPath => pickPath(idx, pIdx, dotPath)}
                          />
                        )}
                      </div>
                    ))}
                    <button type="button" className="path-add" onClick={() => addPath(idx)}>
                      + Add Path
                    </button>
                  </div>
                )}
                {isJson && !isNode && (
                  <span className="field-hint">JSON path only available for node sources.</span>
                )}
              </div>
            )
          })}
        </div>

        <div className="edge-input-add">
          <select
            value={draftSource}
            onChange={e => setDraftSource(e.target.value)}
            aria-label="New source"
          >
            {sourceOptions.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
          </select>
          <select
            value={draftFormat}
            onChange={e => setDraftFormat(e.target.value as 'text' | 'json')}
            aria-label="New source format"
          >
            <option value="text">Text</option>
            <option value="json">JSON</option>
          </select>
          {draftFormat === 'json' && (
            <div className="edge-input-path-block">
              <div className="edge-input-path-head">
                <input
                  className="edge-input-path"
                  value={draftPath}
                  placeholder="Subtree path e.g. items[0].name"
                  onChange={e => setDraftPath(e.target.value)}
                  aria-label="New source subtree path"
                />
                {schemaOf(draftSource) && (
                  <button
                    type="button"
                    className="schema-tree-open"
                    onClick={() => setDraftPathManual(v => !v)}
                    aria-label="Toggle tree path selection"
                  >
                    Tree
                  </button>
                )}
              </div>
              {draftPathManual && schemaOf(draftSource) && (
                <SchemaPathPicker
                  schemaRaw={schemaOf(draftSource)}
                  value={draftPath}
                  onSelect={dotPath => setDraftPath(dotPath)}
                />
              )}
            </div>
          )}
          <button type="button" onClick={addInput}>Add Source</button>
        </div>

        <div className="schema-popup-actions">
          <button type="button" onClick={() => onSave(inputs)}>Save</button>
          <button type="button" className="danger" onClick={onClose}>Cancel</button>
        </div>
      </div>
    </div>
  )
}
