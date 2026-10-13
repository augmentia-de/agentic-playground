import { Component, OnInit, computed, inject, input, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../core/api.service';
import { ValidateResult, WorkflowDoc } from '../core/models';
import { ToastService } from '../core/toast.service';
import { AgentRefOption, GraphEditorComponent } from '../ui/graph-editor';

@Component({
  selector: 'af-workflow-detail',
  imports: [FormsModule, GraphEditorComponent],
  template: `
    <div class="page-head">
      <div>
        <h1>{{ name() || 'Workflow' }}</h1>
        <p class="subtitle mono">{{ id() }}</p>
      </div>
      <div class="toolbar">
        <button class="primary" (click)="validate()" [disabled]="busy()">Validieren</button>
        <button (click)="save()" [disabled]="busy()">Speichern</button>
        <button class="ghost" (click)="format()">Formatieren</button>
        <button class="ghost" (click)="reset()">Zurücksetzen</button>
        <button class="ghost" (click)="download()">Download</button>
      </div>
    </div>

    <div class="tabs">
      <button [class.active]="tab() === 'graph'" (click)="tab.set('graph')">Graph</button>
      <button [class.active]="tab() === 'json'" (click)="tab.set('json')">JSON</button>
      <button [class.active]="tab() === 'versions'" (click)="openVersions()">Versionen</button>
    </div>

    @if (validation(); as v) {
      @if (v.valid) {
        <p class="small" style="color: var(--ok)">✓ gültig — {{ v.summary?.name }} ({{ v.summary?.nodeCount }} Nodes)</p>
      } @else {
        <div class="empty" style="text-align: left">
          <strong>Ungültig:</strong>
          <ul>
            @for (e of v.errors || []; track e) {
              <li class="mono small">{{ e }}</li>
            }
          </ul>
        </div>
      }
    }

    @if (tab() === 'graph') {
      <af-graph-editor [nodesJson]="graphNodes()" [workflowId]="id() ?? ''" [agents]="agentRefOptions()" (nodesChange)="applyNodes($event)"></af-graph-editor>
      <p class="small dim">Drag = Position (nur Ansicht). Änderungen werden sofort ins JSON übernommen.</p>
    }

    @if (tab() === 'json') {
      <div class="card">
        <textarea class="editor" [(ngModel)]="text" [ngModelOptions]="{ standalone: true }" spellcheck="false"></textarea>
        <p class="small dim">Speichern = Runtime-Override + Datei-Persistenz (Version-Snapshot) → sofort unter Runs ausführbar.</p>
      </div>
    }

    @if (tab() === 'versions') {
      <div class="card">
        @if (versions().length) {
          <table>
            <thead><tr><th>Version</th><th></th></tr></thead>
            <tbody>
              @for (v of versions(); track v) {
                <tr>
                  <td class="mono">{{ v }}</td>
                  <td><button class="small" (click)="loadVersion(v)">In Editor laden</button></td>
                </tr>
              }
            </tbody>
          </table>
        } @else {
          <div class="empty">Keine Version-Snapshots (noch keine Überschreibung gespeichert).</div>
        }
      </div>
    }
  `,
})
export class WorkflowDetailPage implements OnInit {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);

  readonly id = input<string>();

  readonly doc = signal<WorkflowDoc | null>(null);
  readonly graphNodes = signal<unknown[]>([]);
  /** Selectable agent refs = the document's inline agents[]. */
  readonly agentRefOptions = computed<AgentRefOption[]>(() =>
    (((this.doc()?.['agents'] as { id?: string; name?: string }[] | undefined) ?? [])
      .filter((a) => !!a.id)
      .map((a) => ({ id: a.id as string, name: a.name }))),
  );
  readonly validation = signal<ValidateResult | null>(null);
  readonly versions = signal<string[]>([]);
  readonly busy = signal(false);
  readonly tab = signal<'graph' | 'json' | 'versions'>('graph');
  text = '';

  ngOnInit(): void {
    this.load();
  }

  private apply(d: WorkflowDoc): void {
    this.doc.set(d);
    this.text = JSON.stringify(d, null, 2);
    const wf = d['mainWorkflow'] as { nodes?: unknown[] } | undefined;
    this.graphNodes.set((wf?.nodes as unknown[]) ?? []);
  }

  load(): void {
    const id = this.id();
    if (!id) {
      return;
    }
    this.api.workflow(id).subscribe({
      next: (d) => this.apply(d),
      error: (err) => this.toast.error('Laden fehlgeschlagen: ' + (err?.message ?? err)),
    });
  }

  name(): string {
    return (this.doc()?.['mainWorkflow'] as { name?: string })?.name ?? '';
  }

  applyNodes(nodes: unknown[]): void {
    this.graphNodes.set(nodes);
    const base = JSON.parse(JSON.stringify(this.doc() ?? {}));
    base.mainWorkflow = base.mainWorkflow ?? {};
    base.mainWorkflow.nodes = nodes;
    this.text = JSON.stringify(base, null, 2);
  }

  openVersions(): void {
    this.tab.set('versions');
    const id = this.id();
    if (!id) return;
    this.api.workflowVersions(id).subscribe({ next: (v) => this.versions.set(v), error: () => this.versions.set([]) });
  }

  loadVersion(version: string): void {
    const id = this.id();
    if (!id) return;
    this.api.workflowVersion(id, version).subscribe({
      next: (d) => {
        this.apply(d);
        this.tab.set('json');
        this.toast.info('Version geladen — nicht gespeichert');
      },
      error: (err) => this.toast.error('Version laden fehlgeschlagen: ' + (err?.message ?? err)),
    });
  }

  private parse(): WorkflowDoc | null {
    try {
      const parsed = JSON.parse(this.text);
      if (typeof parsed !== 'object' || parsed === null) {
        throw new Error('kein Objekt');
      }
      return parsed as WorkflowDoc;
    } catch (e) {
      this.toast.error('JSON fehlerhaft: ' + (e as Error).message);
      return null;
    }
  }

  validate(): void {
    const doc = this.parse();
    if (!doc) return;
    this.busy.set(true);
    this.api.validateWorkflow(doc).subscribe({
      next: (v) => {
        this.validation.set(v);
        this.busy.set(false);
        v.valid ? this.toast.success('Definition gültig') : this.toast.error('Definition ungültig');
      },
      error: (err) => {
        this.busy.set(false);
        this.validation.set({ valid: false, errors: [String(err?.message ?? err)] });
      },
    });
  }

  save(): void {
    const id = this.id();
    const doc = this.parse();
    if (!id || !doc) return;
    this.busy.set(true);
    this.api.saveWorkflow(id, doc).subscribe({
      next: () => {
        this.busy.set(false);
        this.toast.success('Gespeichert (Override + Datei + Version)');
        this.load();
      },
      error: (err) => {
        this.busy.set(false);
        this.validation.set({ valid: false, errors: [String(err?.error?.message ?? err?.message ?? err)] });
        this.toast.error('Speichern fehlgeschlagen');
      },
    });
  }

  format(): void {
    const doc = this.parse();
    if (doc) {
      this.text = JSON.stringify(doc, null, 2);
    }
  }

  reset(): void {
    this.load();
    this.validation.set(null);
  }

  download(): void {
    const blob = new Blob([this.text], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = (this.id() ?? 'workflow') + '.json';
    a.click();
    URL.revokeObjectURL(url);
  }
}
