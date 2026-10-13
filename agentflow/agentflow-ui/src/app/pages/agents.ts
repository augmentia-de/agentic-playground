import { Component, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { ApiService } from '../core/api.service';
import { ToastService } from '../core/toast.service';

interface AgentRow {
  docId: string;
  agent: any;
}

@Component({
  selector: 'af-agents',
  imports: [FormsModule, RouterLink],
  template: `
    <div class="page-head">
      <div>
        <h1>Agents</h1>
        <p class="subtitle">Agenten aus allen Workflow-Definitionen bearbeiten/anlegen (agents[]).</p>
      </div>
      <div class="toolbar">
        <select [(ngModel)]="newDocId" [ngModelOptions]="{ standalone: true }" style="max-width: 220px">
          @for (d of defs(); track d.id) {
            <option [ngValue]="d.id">{{ d.id }}</option>
          }
        </select>
        <button class="primary" (click)="newAgent()" [disabled]="!newDocId">+ Neuer Agent</button>
      </div>
    </div>

    <div class="grid cols-2">
      <div>
        <div class="card">
          @if (rows().length) {
            <table>
              <thead>
                <tr><th>ID</th><th>Name</th><th>Model</th><th>Guardrails</th><th>Def</th></tr>
              </thead>
              <tbody>
                @for (r of rows(); track r.docId + '/' + r.agent.id) {
                  <tr (click)="select(r)" style="cursor: pointer">
                    <td class="mono">{{ r.agent.id }}</td>
                    <td>{{ r.agent.name }}</td>
                    <td class="small">{{ r.agent.model || '–' }}</td>
                    <td class="small">{{ guardrailSummary(r.agent) }}</td>
                    <td class="mono small"><a [routerLink]="['/workflows', r.docId]">{{ r.docId }}</a></td>
                  </tr>
                }
              </tbody>
            </table>
          } @else {
            <div class="empty">Keine Agenten gefunden.</div>
          }
        </div>
      </div>

      <div>
        <div class="card">
          @if (draft(); as a) {
            <h3>{{ isNew() ? 'Neuer Agent' : 'Bearbeiten' }}: {{ a.id }}</h3>
            <div class="field"><label>ID</label><input [(ngModel)]="a.id" [ngModelOptions]="{ standalone: true }" [disabled]="!isNew()" /></div>
            <div class="field"><label>Name</label><input [(ngModel)]="a.name" [ngModelOptions]="{ standalone: true }" /></div>
            <div class="row">
              <div class="field" style="flex: 1"><label>Model</label><input [(ngModel)]="a.model" [ngModelOptions]="{ standalone: true }" placeholder="(Default aus Config)" /></div>
              <div class="field" style="width: 110px"><label>Temperature</label><input type="number" step="0.1" [(ngModel)]="a.temperature" [ngModelOptions]="{ standalone: true }" /></div>
              <div class="field" style="width: 120px"><label>Max tokens</label><input type="number" [(ngModel)]="a.maxTokens" [ngModelOptions]="{ standalone: true }" /></div>
            </div>
            <div class="field"><label>Tools (kommagetrennt)</label><input [(ngModel)]="toolsCsv" [ngModelOptions]="{ standalone: true }" placeholder="readFile,listDirectory" /></div>
            <div class="field"><label>System Prompt</label><textarea [(ngModel)]="a.systemPrompt" [ngModelOptions]="{ standalone: true }"></textarea></div>

            <h4 class="small dim" style="margin: 12px 0 4px">Guardrails &amp; Hooks</h4>
            <div class="row">
              <div class="field" style="flex: 1">
                <label>Guardrails (Input)</label>
                <select multiple size="3" [(ngModel)]="guardrailsInputSel" [ngModelOptions]="{ standalone: true }">
                  @for (g of guardrailNames; track g) { <option [ngValue]="g">{{ g }}</option> }
                </select>
              </div>
              <div class="field" style="flex: 1">
                <label>Guardrails (Output)</label>
                <select multiple size="3" [(ngModel)]="guardrailsOutputSel" [ngModelOptions]="{ standalone: true }">
                  @for (g of guardrailNames; track g) { <option [ngValue]="g">{{ g }}</option> }
                </select>
              </div>
            </div>
            <div class="row">
              <div class="field" style="flex: 1">
                <label>PII-Masken (Hook „pii")</label>
                <select multiple size="3" [(ngModel)]="piiMaskSel" [ngModelOptions]="{ standalone: true }">
                  @for (m of piiMaskTypes; track m) { <option [ngValue]="m">{{ m }}</option> }
                </select>
              </div>
              <div class="field" style="width: 180px">
                <label>Audit</label>
                <label style="margin-top: 6px"><input type="checkbox" [(ngModel)]="auditSel" [ngModelOptions]="{ standalone: true }" style="width: auto" /> Audit-Trail</label>
              </div>
            </div>

            <div class="row">
              <label><input type="checkbox" [(ngModel)]="a.jsonOutput" [ngModelOptions]="{ standalone: true }" style="width: auto" /> JSON Output</label>
              <label><input type="checkbox" [(ngModel)]="a.active" [ngModelOptions]="{ standalone: true }" style="width: auto" /> Aktiv</label>
            </div>
            <div class="toolbar" style="margin-top: 12px">
              <button class="primary" (click)="save()" [disabled]="busy()">Speichern</button>
              <button class="ghost" (click)="testRun()" [disabled]="busy()">Test-Run (quad)</button>
            </div>
          } @else {
            <div class="empty">Agent links auswählen oder oben „+ Neuer Agent".</div>
          }
        </div>
      </div>
    </div>
  `,
})
export class AgentsPage {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);

  readonly rows = signal<AgentRow[]>([]);
  readonly defs = signal<{ id: string; name: string }[]>([]);
  readonly draft = signal<any | null>(null);
  readonly busy = signal(false);
  readonly isNew = signal(false);

  toolsCsv = '';
  newDocId = '';
  guardrailNames: string[] = [];
  piiMaskTypes: string[] = [];
  guardrailsInputSel: string[] = [];
  guardrailsOutputSel: string[] = [];
  piiMaskSel: string[] = [];
  auditSel = false;

  private readonly docs = new Map<string, any>();
  private selectedDocId = '';

  constructor() {
    this.reload();
    this.api.quadCapabilities().subscribe({
      next: (c) => {
        this.guardrailNames = (c['guardrails'] as string[]) ?? [];
        this.piiMaskTypes = (c['piiMaskTypes'] as string[]) ?? [];
      },
      error: () => {},
    });
  }

  reload(): void {
    this.api.workflows().subscribe({
      next: (defs) => {
        this.defs.set(defs.map((d) => ({ id: d.id, name: d.name })));
        if (!this.newDocId && defs.length) {
          this.newDocId = defs[0].id;
        }
        defs.forEach((d) => this.loadDoc(d.id));
      },
      error: (err) => this.toast.error('Laden fehlgeschlagen: ' + (err?.message ?? err)),
    });
  }

  private loadDoc(id: string): void {
    this.api.workflow(id).subscribe({
      next: (doc) => {
        this.docs.set(id, doc);
        const agents = ((doc['agents'] as any[]) ?? []).map((agent) => ({ docId: id, agent }));
        this.rows.update((list) => [...list.filter((r) => r.docId !== id), ...agents]);
      },
      error: () => {},
    });
  }

  /** Human-readable guardrail/hook summary for the list. */
  guardrailSummary(agent: any): string {
    const parts: string[] = [];
    if ((agent?.guardrailsInput as string[])?.length) {
      parts.push('in: ' + (agent.guardrailsInput as string[]).join(','));
    }
    if ((agent?.guardrailsOutput as string[])?.length) {
      parts.push('out: ' + (agent.guardrailsOutput as string[]).join(','));
    }
    if ((agent?.hooks as string[])?.length) {
      parts.push('hooks: ' + (agent.hooks as string[]).join(','));
    }
    return parts.join(' · ') || '–';
  }

  select(row: AgentRow): void {
    this.selectedDocId = row.docId;
    const clone = JSON.parse(JSON.stringify(row.agent));
    this.isNew.set(false);
    this.draft.set(clone);
    this.toolsCsv = ((clone.tools as string[]) ?? []).join(', ');
    this.guardrailsInputSel = [...((clone.guardrailsInput as string[]) ?? [])];
    this.guardrailsOutputSel = [...((clone.guardrailsOutput as string[]) ?? [])];
    const hooks: string[] = (clone.hooks as string[]) ?? [];
    this.piiMaskSel = this.parsePiiMasks(hooks);
    this.auditSel = hooks.some((h) => h.split(':')[0].trim().toLowerCase() === 'audit');
  }

  newAgent(): void {
    if (!this.newDocId) {
      return;
    }
    const doc = this.docs.get(this.newDocId);
    const existing = ((doc?.['agents'] as any[]) ?? []).map((a) => a.id as string);
    const id = this.uniqueId('agent', existing);
    this.selectedDocId = this.newDocId;
    this.isNew.set(true);
    this.toolsCsv = '';
    this.guardrailsInputSel = [];
    this.guardrailsOutputSel = [];
    this.piiMaskSel = [];
    this.auditSel = false;
    this.draft.set({
      id,
      name: 'Neuer Agent',
      version: '1.0.0',
      category: 'custom',
      model: '',
      temperature: 0.2,
      maxTokens: 1024,
      topP: 1.0,
      tools: [],
      guardrailsInput: [],
      guardrailsOutput: [],
      hooks: [],
      systemPrompt: '',
      jsonOutput: false,
      active: true,
    });
  }

  private parsePiiMasks(hooks: string[]): string[] {
    for (const h of hooks) {
      const [name, args] = h.split(':');
      if (name?.trim().toLowerCase() === 'pii') {
        return args ? args.split(',').map((s) => s.trim()).filter(Boolean) : [];
      }
    }
    return [];
  }

  private uniqueId(prefix: string, existing: string[]): string {
    let i = 1;
    let id = `${prefix}-1`;
    while (existing.includes(id)) {
      id = `${prefix}-${++i}`;
    }
    return id;
  }

  save(): void {
    const agent = this.draft();
    const doc = this.docs.get(this.selectedDocId);
    if (!agent || !doc) {
      this.toast.error('Keine Definition geladen.');
      return;
    }
    agent.tools = this.toolsCsv
      .split(',')
      .map((t) => t.trim())
      .filter((t) => t.length > 0);
    agent.guardrailsInput = [...this.guardrailsInputSel];
    agent.guardrailsOutput = [...this.guardrailsOutputSel];
    const hooks: string[] = [];
    if (this.piiMaskSel.length) {
      hooks.push('pii:' + this.piiMaskSel.join(','));
    }
    if (this.auditSel) {
      hooks.push('audit');
    }
    agent.hooks = hooks;

    const agents = (doc['agents'] as any[]) ?? [];
    const index = agents.findIndex((a) => a.id === agent.id);
    if (index >= 0) {
      agents[index] = agent;
    } else {
      agents.push(agent);
    }
    doc['agents'] = agents;
    this.busy.set(true);
    this.api.saveWorkflow(this.selectedDocId, doc).subscribe({
      next: () => {
        this.busy.set(false);
        this.isNew.set(false);
        this.toast.success('Agent gespeichert (Runtime-Override)');
        this.rows.update((list) => {
          const rest = list.filter((r) => !(r.docId === this.selectedDocId && r.agent.id === agent.id));
          return [...rest, { docId: this.selectedDocId, agent }];
        });
      },
      error: (err) => {
        this.busy.set(false);
        this.toast.error('Speichern fehlgeschlagen: ' + (err?.error?.message ?? err?.message ?? err));
      },
    });
  }

  testRun(): void {
    const agent = this.draft();
    if (!agent?.systemPrompt) {
      this.toast.error('Kein System Prompt zum Testen.');
      return;
    }
    this.api.start('/agentflow/runs/quad', { input: agent.systemPrompt }).subscribe({
      next: (r) => this.toast.success('Test-Run gestartet: ' + (r['workflowId'] ?? '')),
      error: (err) => this.toast.error('Test-Run fehlgeschlagen: ' + (err?.message ?? err)),
    });
  }
}
