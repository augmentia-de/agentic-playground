import { useState } from 'react'
import { buildSchemaTree } from '../../services/jsonSchema'
import type { SchemaTreeNode } from '../../services/jsonSchema'

interface SchemaPathPickerProps {
  schemaRaw: string
  value: string
  onSelect: (dotPath: string) => void
}

/** Recursive, collapsible tree navigation to visually pick a subtree path in the JSON schema. */
export function SchemaPathPicker({ schemaRaw, value, onSelect }: SchemaPathPickerProps) {
  const tree = buildSchemaTree(schemaRaw)
  const [open, setOpen] = useState<Record<string, boolean>>(() => {
    const initial: Record<string, boolean> = {}
    for (const n of tree) initial[n.dotPath] = true
    return initial
  })

  if (tree.length === 0) {
    return (
      <span className="field-hint">
        Kein JSON-Schema angegeben — Pfad manuell eingeben.
      </span>
    )
  }

  const renderNode = (node: SchemaTreeNode) => {
    const hasChildren = node.children.length > 0
    const isOpen = !!open[node.dotPath]
    const isSelected = value === node.dotPath
    return (
      <li key={node.dotPath}>
        <div className={`schema-tree-row ${isSelected ? 'selected' : ''}`}>
          {hasChildren ? (
            <button
              type="button"
              className="schema-tree-toggle"
              aria-label={isOpen ? `Collapse ${node.key}` : `Expand ${node.key}`}
              onClick={() => setOpen(prev => ({ ...prev, [node.dotPath]: !prev[node.dotPath] }))}
            >
              {isOpen ? '▾' : '▸'}
            </button>
          ) : (
            <span className="schema-tree-toggle placeholder" />
          )}
          <button
            type="button"
            className="schema-tree-label"
            onClick={() => onSelect(node.dotPath)}
            title={node.dotPath}
          >
            <span className="schema-tree-key">{node.key}</span>
            {node.type && <span className="schema-tree-type">{node.type}</span>}
          </button>
        </div>
        {hasChildren && isOpen && (
          <ul className="schema-tree-children">
            {node.children.map(renderNode)}
          </ul>
        )}
      </li>
    )
  }

  return (
    <ul className="schema-tree">
      {tree.map(renderNode)}
    </ul>
  )
}
