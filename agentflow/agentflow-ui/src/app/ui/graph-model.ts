/**
 * Pure view-model + (de)serialisation helpers for {@link GraphEditorComponent}.
 *
 * Extracted from `graph-editor.ts` (WP12) so the node mapping and auto-layout can be
 * unit-tested without booting the Angular component.
 */

export interface BranchVM {
  branchId: string;
  condition: string;
  target: string;
}

/** An agent selectable as a node reference (from the document's inline agents[]). */
export interface AgentRefOption {
  id: string;
  name?: string;
}

export interface NodeVM {
  id: string;
  type: string;
  title: string;
  agentRef: string;
  dependsOn: string[];
  requiresHumanApproval: boolean;
  assignee: string;
  maxIterations: number;
  exitCondition: string;
  bodySteps: string[];
  joinType: string;
  joinN: number | null;
  branches: BranchVM[];
  defaultTarget: string;
}

export interface Point {
  x: number;
  y: number;
}

export function emptyVm(id: string, type: string): NodeVM {
  return {
    id,
    type,
    title: id,
    agentRef: '',
    dependsOn: [],
    requiresHumanApproval: false,
    assignee: 'human',
    maxIterations: 3,
    exitCondition: '',
    bodySteps: [],
    joinType: 'all',
    joinN: null,
    branches: [],
    defaultTarget: '',
  };
}

/** Maps a schema-shaped workflow node to the editor's editable view model. */
export function toNodeVm(n: Record<string, any>): NodeVM {
  const branches = ((n['branches'] as any[] | undefined) ?? []).map((b) => ({
    branchId: b.branchId ?? '',
    condition: b.condition ?? '',
    target: b.target ?? b.firstStep ?? '',
  }));
  return {
    id: n['id'],
    type: n['type'],
    title: n['title'] ?? '',
    agentRef: (n['agent'] as any)?.ref ?? '',
    dependsOn: (n['dependsOn'] as string[]) ?? [],
    requiresHumanApproval: !!n['requiresHumanApproval'],
    assignee: n['assignee'] ?? 'human',
    maxIterations: n['maxIterations'] ?? 3,
    exitCondition: n['exitCondition'] ?? '',
    bodySteps: (n['bodySteps'] as string[]) ?? [],
    joinType: n['joinType'] ?? 'all',
    joinN: n['joinN'] ?? null,
    branches,
    defaultTarget: n['defaultTarget'] ?? '',
  };
}

/** Serialises a view-model node back to the schema-shaped node array. */
export function toNodeJson(v: NodeVM): Record<string, unknown> {
  const node: Record<string, unknown> = { id: v.id, type: v.type };
  if (v.title) node['title'] = v.title;
  if (v.dependsOn.length) node['dependsOn'] = v.dependsOn;

  switch (v.type) {
    case 'agent':
      node['agent'] = { ref: v.agentRef };
      if (v.requiresHumanApproval) node['requiresHumanApproval'] = true;
      break;
    case 'decision':
      node['branches'] = v.branches.length ? v.branches : [{ branchId: 'b1', condition: '', target: v.defaultTarget }];
      if (v.defaultTarget) node['defaultTarget'] = v.defaultTarget;
      break;
    case 'fork':
      node['branches'] = v.branches.length ? v.branches : [{ branchId: 'b1', condition: '', target: '' }];
      break;
    case 'loop':
      node['maxIterations'] = v.maxIterations || 1;
      node['exitCondition'] = v.exitCondition || 'falsy';
      node['bodySteps'] = v.bodySteps;
      break;
    case 'join':
      node['joinType'] = v.joinType || 'all';
      if (v.joinType === 'n_of_m' && v.joinN) node['joinN'] = v.joinN;
      break;
    case 'hitl':
      node['assignee'] = v.assignee || 'human';
      break;
    case 'subworkflow':
      node['workflowRef'] = v.agentRef || '';
      break;
    default:
      break;
  }
  return node;
}

/**
 * Layered left-to-right auto-layout: column = longest dependency distance, row = insertion order
 * within the column.
 */
export function autoLayout(nodes: NodeVM[]): Record<string, Point> {
  const distances: Record<string, number> = {};
  const visit = (id: string, seen: Set<string>): number => {
    if (distances[id] !== undefined) return distances[id];
    if (seen.has(id)) return 0; // cycle guard
    seen.add(id);
    const node = nodes.find((n) => n.id === id);
    const parents = node?.dependsOn ?? [];
    const d = parents.length ? 1 + Math.max(...parents.map((p) => visit(p, seen))) : 0;
    distances[id] = d;
    return d;
  };
  const rows: Record<number, number> = {};
  const out: Record<string, Point> = {};
  for (const node of nodes) {
    const d = visit(node.id, new Set());
    const row = rows[d] ?? 0;
    rows[d] = row + 1;
    out[node.id] = { x: 40 + d * 250, y: 40 + row * 110 };
  }
  return out;
}
