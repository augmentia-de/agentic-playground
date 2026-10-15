/**
 * Pure graph helpers used by the workflow composer.
 *
 * Extracted from `WorkflowComposer.tsx` (WP12) so the reachability logic can be unit-tested
 * and reused by the inspector components.
 */

/** Forward reachability: all nodes reachable from a source via outgoing edges. */
export function getDownstreamNodeIds(
  sourceId: string,
  edges: Array<{ source: string; target: string }>,
): Set<string> {
  const result = new Set<string>()
  const queue = [sourceId]
  while (queue.length > 0) {
    const cur = queue.shift()!
    for (const edge of edges) {
      if (edge.source === cur && !result.has(edge.target)) {
        result.add(edge.target)
        queue.push(edge.target)
      }
    }
  }
  return result
}

/** Get all downstream agent/other non-control-flow nodes of a given source node. */
export function getAvailableTargets(
  sourceId: string,
  nodes: Array<{ id: string; title: string; type: string }>,
  edges: Array<{ source: string; target: string }>,
): Array<{ id: string; title: string }> {
  const downstream = getDownstreamNodeIds(sourceId, edges)
  const targets: Array<{ id: string; title: string }> = []
  for (const n of nodes) {
    if (downstream.has(n.id)) {
      targets.push({ id: n.id, title: n.title })
    }
  }
  return targets
}

/** Selectable SessionState fields for edge input sources. */
export const SESSION_FIELDS = [
  { value: '__session.findings', label: 'Findings (SessionState)' },
  { value: '__session.currentProject', label: 'currentProject' },
  { value: '__session.tenantId', label: 'tenantId' },
  { value: '__session.cwd', label: 'CWD' },
]

/** Special source = the workflow's start/initial task (workflow-state object). */
export const INITIAL_DATA_FIELD = { value: '__initialData', label: 'Initial Data (Start-Task)' }
