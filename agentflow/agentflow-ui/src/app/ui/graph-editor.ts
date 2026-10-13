import { Component, computed, effect, input, output, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';

interface BranchVM {
  branchId: string;
  condition: string;
  target: string;
}

/** An agent selectable as a node reference (from the document's inline agents[]). */
export interface AgentRefOption {
  id: string;
  name?: string;
}

interface NodeVM {
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

interface Point {
  x: number;
  y: number;
}

/**
 * Visual editor for {@code mainWorkflow.nodes}: layered layout, drag (positions per workflow in
 * localStorage), dependency editing via checkboxes and click-to-connect, add/rename/delete with
 * reference integrity, and target dropdowns for decisions/forks/loops. Emits the rebuilt,
 * schema-shaped node array.
 */
@Component({
  selector: 'af-graph-editor',
  imports: [FormsModule],
  template: `
    <div class="graph-wrap">
      <div class="graph-canvas" (pointermove)="onMove($event)" (pointerup)="endDrag()" (pointerleave)="endDrag()">
        <div class="graph-content" [style.width.px]="content().w" [style.height.px]="content().h">
          <svg class="edges" [attr.width]="content().w" [attr.height]="content().h">
            @for (e of edges(); track e.id) {
              <line [attr.x1]="e.x1" [attr.y1]="e.y1" [attr.x2]="e.x2" [attr.y2]="e.y2" />
            }
          </svg>
          @for (n of nodes(); track n.id) {
            <div
              class="gnode"
              [class.selected]="selectedId() === n.id"
              [class.link-source]="linkSource() === n.id"
              [style.left.px]="point(n.id).x"
              [style.top.px]="point(n.id).y"
              (pointerdown)="startDrag(n.id, $event)"
              (click)="onNodeClick(n.id)"
            >
              <span class="chip">{{ n.type }}</span>
              <strong>{{ n.title || n.id }}</strong>
              <span class="mono small">{{ n.id }}</span>
              @if (n.requiresHumanApproval) {
                <span class="badge warn">HITL</span>
              }
            </div>
          }
        </div>
      </div>

      <div class="graph-side">
        <div class="toolbar">
          <select [(ngModel)]="newType" [ngModelOptions]="{ standalone: true }">
            @for (t of nodeTypes; track t) {
              <option [ngValue]="t">{{ t }}</option>
            }
          </select>
          <button class="small" (click)="addNode()">+ Node</button>
          <button class="small" (click)="addConnected()" [disabled]="!selectedId()">+ verbunden</button>
          <button class="small" [class.primary]="linking()" (click)="linking.set(!linking())">
            🔗 {{ linking() ? 'Klick: Quelle→Ziel' : 'Verbinden' }}
          </button>
          <button class="small danger" (click)="deleteSelected()" [disabled]="!selected()">Löschen</button>
          <button class="small ghost" (click)="resetLayout()" title="Gespeicherte Positionen verwerfen und Auto-Layout verwenden">Layout zurücksetzen</button>
        </div>

        @if (selected(); as s) {
          <div class="card">
            <div class="field"><label>ID</label><input [(ngModel)]="s.id" [ngModelOptions]="{ standalone: true }" (change)="rename()" /></div>
            <div class="field"><label>Titel</label><input [(ngModel)]="s.title" [ngModelOptions]="{ standalone: true }" (change)="update()" /></div>
            <div class="field">
              <label>Typ</label>
              <select [(ngModel)]="s.type" [ngModelOptions]="{ standalone: true }" (change)="update()">
                @for (t of nodeTypes; track t) {
                  <option [ngValue]="t">{{ t }}</option>
                }
              </select>
            </div>

            <div class="field">
              <label>Dependencies (ankommend)</label>
              <div class="deps">
                @for (id of nodeIds(); track id) {
                  @if (id !== s.id) {
                    <label class="dep">
                      <input type="checkbox" [checked]="s.dependsOn.includes(id)" (change)="toggleDep(id, $event)" />
                      <span class="mono">{{ id }}</span>
                    </label>
                  }
                } @empty {
                  <span class="small dim">keine anderen Knoten</span>
                }
              </div>
            </div>

            @if (s.type === 'agent') {
              <div class="field">
                <label>Agent-Ref</label>
                <select [(ngModel)]="s.agentRef" [ngModelOptions]="{ standalone: true }" (change)="update()">
                  <option value="">— Agent wählen —</option>
                  @if (s.agentRef && !hasAgent(s.agentRef)) {
                    <option [ngValue]="s.agentRef">{{ s.agentRef }} (nicht im Dokument)</option>
                  }
                  @for (a of agents(); track a.id) {
                    <option [ngValue]="a.id">{{ a.name ? a.name + ' (' + a.id + ')' : a.id }}</option>
                  }
                </select>
                @if (!agents().length) {
                  <p class="small dim">Keine Agents im Dokument — unter „Agents" anlegen.</p>
                }
              </div>
              <label><input type="checkbox" [(ngModel)]="s.requiresHumanApproval" [ngModelOptions]="{ standalone: true }" style="width: auto" (change)="update()" /> requiresHumanApproval</label>
            }
            @if (s.type === 'hitl') {
              <div class="field"><label>Assignee</label><input [(ngModel)]="s.assignee" [ngModelOptions]="{ standalone: true }" (change)="update()" /></div>
            }
            @if (s.type === 'loop') {
              <div class="field"><label>maxIterations</label><input type="number" [(ngModel)]="s.maxIterations" [ngModelOptions]="{ standalone: true }" (change)="update()" /></div>
              <div class="field"><label>exitCondition</label><input [(ngModel)]="s.exitCondition" [ngModelOptions]="{ standalone: true }" (change)="update()" /></div>
              <div class="field">
                <label>bodySteps (Region)</label>
                <div class="deps">
                  @for (id of nodeIds(); track id) {
                    @if (id !== s.id) {
                      <label class="dep">
                        <input type="checkbox" [checked]="s.bodySteps.includes(id)" (change)="toggleBody(id, $event)" />
                        <span class="mono">{{ id }}</span>
                      </label>
                    }
                  }
                </div>
              </div>
            }
            @if (s.type === 'join') {
              <div class="field"><label>joinType</label>
                <select [(ngModel)]="s.joinType" [ngModelOptions]="{ standalone: true }" (change)="update()">
                  <option value="all">all</option><option value="any">any</option><option value="n_of_m">n_of_m</option>
                </select>
              </div>
              @if (s.joinType === 'n_of_m') {
                <div class="field"><label>joinN</label><input type="number" [(ngModel)]="s.joinN" [ngModelOptions]="{ standalone: true }" (change)="update()" /></div>
              }
            }
            @if (s.type === 'decision' || s.type === 'fork') {
              <div class="field">
                <label>Branches</label>
                @for (b of s.branches; track $index) {
                  <div class="branch-row">
                    @if (s.type === 'decision') {
                      <input [(ngModel)]="b.condition" [ngModelOptions]="{ standalone: true }" placeholder="contains:..." (change)="update()" />
                    }
                    <select [(ngModel)]="b.target" [ngModelOptions]="{ standalone: true }" (change)="update()">
                      <option value="">— Ziel —</option>
                      @for (id of nodeIds(); track id) {
                        <option [ngValue]="id">{{ id }}</option>
                      }
                    </select>
                    <button class="small ghost" (click)="removeBranch($index)">✕</button>
                  </div>
                }
                <button class="small" (click)="addBranch()">+ Branch</button>
              </div>
              @if (s.type === 'decision') {
                <div class="field"><label>defaultTarget</label>
                  <select [(ngModel)]="s.defaultTarget" [ngModelOptions]="{ standalone: true }" (change)="update()">
                    <option value="">— kein —</option>
                    @for (id of nodeIds(); track id) {
                      <option [ngValue]="id">{{ id }}</option>
                    }
                  </select>
                </div>
              }
            }
          </div>
        } @else {
          <div class="empty">Node auswählen oder hinzufügen.</div>
        }
      </div>
    </div>
  `,
  styles: [
    `
      .graph-wrap { display: grid; grid-template-columns: 1fr 360px; gap: 12px; }
      .graph-canvas { position: relative; min-height: 460px; border: 1px solid var(--border); border-radius: 10px; background: var(--bg); overflow: auto; }
      .graph-content { position: relative; }
      .edges { position: absolute; left: 0; top: 0; pointer-events: none; }
      .edges line { stroke: var(--text-dim); stroke-width: 1.5; opacity: 0.6; }
      .gnode { position: absolute; display: flex; flex-direction: column; gap: 2px; width: 200px; padding: 8px 10px; background: var(--bg-elev); border: 1px solid var(--border); border-radius: 8px; cursor: grab; }
      .gnode.selected { border-color: var(--primary); box-shadow: 0 0 0 2px color-mix(in srgb, var(--primary) 40%, transparent); }
      .gnode.link-source { border-color: var(--warn); }
      .graph-side { display: flex; flex-direction: column; gap: 10px; }
      .deps { display: grid; grid-template-columns: 1fr 1fr; gap: 4px; max-height: 160px; overflow: auto; }
      .dep { display: flex; align-items: center; gap: 6px; font-size: 12px; }
      .dep input { width: auto; }
      .branch-row { display: flex; gap: 6px; margin-bottom: 6px; align-items: center; }
    `,
  ],
})
export class GraphEditorComponent {
  readonly nodesJson = input<unknown[]>([]);
  readonly workflowId = input<string>('');
  /** Agents from the document's inline agents[] that an agent node may reference. */
  readonly agents = input<AgentRefOption[]>([]);
  readonly nodesChange = output<unknown[]>();

  readonly nodeTypes = ['agent', 'decision', 'loop', 'fork', 'join', 'hitl', 'subworkflow', 'end'];

  readonly nodes = signal<NodeVM[]>([]);
  readonly selectedId = signal<string | null>(null);
  readonly positions = signal<Record<string, Point>>({});
  readonly linking = signal(false);
  readonly linkSource = signal<string | null>(null);

  private static readonly NODE_W = 200;
  private static readonly NODE_H = 96;

  readonly nodeIds = computed(() => this.nodes().map((n) => n.id));

  /** Bounding box of all nodes; sizes the SVG so edges are never clipped. */
  readonly content = computed(() => {
    let w = 600;
    let h = 460;
    for (const n of this.nodes()) {
      const p = this.point(n.id);
      w = Math.max(w, p.x + GraphEditorComponent.NODE_W + 60);
      h = Math.max(h, p.y + GraphEditorComponent.NODE_H + 60);
    }
    return { w, h };
  });

  newType = 'agent';
  private originalId = '';
  private drag: { id: string; dx: number; dy: number } | null = null;

  constructor() {
    effect(() => {
      this.nodes.set((this.nodesJson() as Record<string, any>[]).map((n) => toVm(n)));
    });
    // Load stored positions only when the workflow changes — not on every node edit, so that
    // structural edits (which re-emit nodes) never clobber the in-memory layout.
    effect(() => {
      const id = this.workflowId();
      this.positions.set(id ? this.loadPositions(id) : {});
    });
    effect(() => {
      const p = this.positions();
      const id = this.workflowId();
      if (id) {
        localStorage.setItem('agentflow.graph.' + id, JSON.stringify(p));
      }
    });
  }

  selected(): NodeVM | undefined {
    const id = this.selectedId();
    return id ? this.nodes().find((n) => n.id === id) : undefined;
  }

  /** Whether an agent id is among the document's selectable agents. */
  hasAgent(id: string): boolean {
    return this.agents().some((a) => a.id === id);
  }

  point(id: string): Point {
    const layout = this.layout();
    return this.positions()[id] ?? layout[id] ?? { x: 40, y: 40 };
  }

  readonly edges = computed(() => {
    const byId = new Set(this.nodes().map((n) => n.id));
    const list: { id: string; x1: number; y1: number; x2: number; y2: number }[] = [];
    for (const n of this.nodes()) {
      const to = this.point(n.id);
      for (const dep of n.dependsOn) {
        if (!byId.has(dep)) continue;
        const from = this.point(dep);
        list.push({ id: `${dep}->${n.id}`, x1: from.x + 100, y1: from.y + 20, x2: to.x + 100, y2: to.y + 20 });
      }
    }
    return list;
  });

  private layout = computed<Record<string, Point>>(() => {
    const nodes = this.nodes();
    const byId = new Map(nodes.map((n) => [n.id, n]));
    const depth = new Map<string, number>();
    const depthOf = (id: string, guard = 0): number => {
      if (depth.has(id)) return depth.get(id)!;
      if (guard > 50) return 0;
      const n = byId.get(id);
      const d = !n || n.dependsOn.length === 0 ? 0 : 1 + Math.max(...n.dependsOn.map((x) => depthOf(x, guard + 1)));
      depth.set(id, d);
      return d;
    };
    const rows: Record<number, number> = {};
    const out: Record<string, Point> = {};
    for (const n of nodes) {
      const d = depthOf(n.id);
      const row = rows[d] ?? 0;
      rows[d] = row + 1;
      out[n.id] = { x: 40 + d * 250, y: 40 + row * 110 };
    }
    return out;
  });

  private loadPositions(id: string): Record<string, Point> {
    try {
      return JSON.parse(localStorage.getItem('agentflow.graph.' + id) ?? '{}');
    } catch {
      return {};
    }
  }

  // ── selection / linking ───────────────────────────────────────────────────

  onNodeClick(id: string): void {
    if (this.linking()) {
      const source = this.linkSource();
      if (!source) {
        this.linkSource.set(id);
      } else if (source !== id) {
        const target = this.nodes().find((n) => n.id === id);
        if (target && !target.dependsOn.includes(source)) {
          target.dependsOn = [...target.dependsOn, source];
        }
        this.linkSource.set(null);
        this.update();
      }
      return;
    }
    this.selectedId.set(id);
    this.originalId = id;
  }

  // ── drag ──────────────────────────────────────────────────────────────────

  startDrag(id: string, event: PointerEvent): void {
    const p = this.point(id);
    const local = this.localPoint(event);
    this.drag = { id, dx: local.x - p.x, dy: local.y - p.y };
    event.stopPropagation();
  }

  onMove(event: PointerEvent): void {
    if (!this.drag) return;
    const local = this.localPoint(event);
    const x = Math.max(0, local.x - this.drag.dx);
    const y = Math.max(0, local.y - this.drag.dy);
    this.positions.update((m) => ({ ...m, [this.drag!.id]: { x, y } }));
  }

  /** Pointer position in canvas-local coordinates (accounting for border and scroll). */
  private localPoint(event: PointerEvent): Point {
    const el = event.currentTarget as HTMLElement;
    const canvas = (el.closest('.graph-canvas') as HTMLElement) ?? el;
    const rect = canvas.getBoundingClientRect();
    return {
      x: event.clientX - rect.left - canvas.clientLeft + canvas.scrollLeft,
      y: event.clientY - rect.top - canvas.clientTop + canvas.scrollTop,
    };
  }

  endDrag(): void {
    this.drag = null;
  }

  /** Discards the stored positions and falls back to the automatic layered layout. */
  resetLayout(): void {
    const id = this.workflowId();
    if (id) {
      localStorage.removeItem('agentflow.graph.' + id);
    }
    this.positions.set({});
  }

  // ── add / delete / rename ─────────────────────────────────────────────────

  private nextId(): string {
    const base = 'node';
    let i = this.nodes().length + 1;
    while (this.nodes().some((n) => n.id === `${base}-${i}`)) i++;
    return `${base}-${i}`;
  }

  addNode(): void {
    const vm = emptyVm(this.nextId(), this.newType);
    this.nodes.update((list) => [...list, vm]);
    this.selectedId.set(vm.id);
    this.originalId = vm.id;
    this.update();
  }

  addConnected(): void {
    const parent = this.selectedId();
    const vm = emptyVm(this.nextId(), this.newType);
    if (parent) vm.dependsOn = [parent];
    this.nodes.update((list) => [...list, vm]);
    this.selectedId.set(vm.id);
    this.originalId = vm.id;
    this.update();
  }

  deleteSelected(): void {
    const s = this.selected();
    if (!s) return;
    const id = s.id;
    this.nodes.update((list) =>
      list
        .filter((n) => n.id !== id)
        .map((n) => ({
          ...n,
          dependsOn: n.dependsOn.filter((d) => d !== id),
          bodySteps: n.bodySteps.filter((b) => b !== id),
          branches: n.branches.filter((b) => b.target !== id),
          defaultTarget: n.defaultTarget === id ? '' : n.defaultTarget,
        })),
    );
    this.selectedId.set(null);
    this.update();
  }

  rename(): void {
    const s = this.selected();
    if (!s) return;
    const oldId = this.originalId;
    const newId = s.id;
    if (newId === oldId) return;
    if (!newId.trim() || this.nodes().some((n) => n !== s && n.id === newId)) {
      s.id = oldId; // reject duplicate/blank
      return;
    }
    for (const n of this.nodes()) {
      n.dependsOn = n.dependsOn.map((d) => (d === oldId ? newId : d));
      n.bodySteps = n.bodySteps.map((b) => (b === oldId ? newId : b));
      n.branches = n.branches.map((b) => ({ ...b, target: b.target === oldId ? newId : b.target }));
      if (n.defaultTarget === oldId) n.defaultTarget = newId;
    }
    this.originalId = newId;
    this.selectedId.set(newId);
    // move stored position
    const pos = this.positions();
    if (pos[oldId]) {
      const { [oldId]: moved, ...rest } = pos;
      this.positions.set({ ...rest, [newId]: moved });
    }
    this.update();
  }

  // ── dependencies / branches ───────────────────────────────────────────────

  toggleDep(id: string, event: Event): void {
    const s = this.selected();
    if (!s) return;
    const checked = (event.target as HTMLInputElement).checked;
    s.dependsOn = checked ? [...new Set([...s.dependsOn, id])] : s.dependsOn.filter((d) => d !== id);
    this.update();
  }

  toggleBody(id: string, event: Event): void {
    const s = this.selected();
    if (!s) return;
    const checked = (event.target as HTMLInputElement).checked;
    s.bodySteps = checked ? [...new Set([...s.bodySteps, id])] : s.bodySteps.filter((b) => b !== id);
    this.update();
  }

  addBranch(): void {
    const s = this.selected();
    if (!s) return;
    s.branches = [...s.branches, { branchId: 'b' + (s.branches.length + 1), condition: '', target: '' }];
    this.update();
  }

  removeBranch(index: number): void {
    const s = this.selected();
    if (!s) return;
    s.branches = s.branches.filter((_, i) => i !== index);
    this.update();
  }

  update(): void {
    this.nodesChange.emit(this.nodes().map((n) => toJson(n)));
  }
}

// ── mapping helpers ─────────────────────────────────────────────────────────

function emptyVm(id: string, type: string): NodeVM {
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

function toVm(n: Record<string, any>): NodeVM {
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

function toJson(v: NodeVM): Record<string, unknown> {
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
