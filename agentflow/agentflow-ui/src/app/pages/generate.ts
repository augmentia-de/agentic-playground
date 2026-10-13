import { Component, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { ApiService } from '../core/api.service';
import { WorkflowDoc } from '../core/models';
import { ToastService } from '../core/toast.service';

@Component({
  selector: 'af-generate',
  imports: [FormsModule, RouterLink],
  template: `
    <div class="page-head">
      <div>
        <h1>Workflow generieren</h1>
        <p class="subtitle">
          Aufgabe per LLM in einen lauffähigen v2-Workflow zerlegen — Vorschau, Speichern, Ausführen.
        </p>
      </div>
    </div>

    <div class="grid cols-2">
      <div class="card">
        <div class="field">
          <label>Aufgabe</label>
          <textarea
            [(ngModel)]="task"
            [ngModelOptions]="{ standalone: true }"
            placeholder="z. B. Prüfe die eingehende Support-Anfrage, recherchiere die passende Playbook-Seite und entwirf eine Antwort."
          ></textarea>
        </div>
        <div class="field">
          <label>Name (optional)</label>
          <input [(ngModel)]="name" [ngModelOptions]="{ standalone: true }" placeholder="Dynamic Workflow" />
        </div>
        <div class="toolbar">
          <button class="primary" (click)="preview()" [disabled]="busy()">Vorschau</button>
          <button (click)="save()" [disabled]="busy() || !doc()">Speichern</button>
          <button (click)="run()" [disabled]="busy() || !doc()">Ausführen</button>
        </div>
        <p class="small dim">
          Der Generator nutzt dieselbe LLM-Konfiguration wie die Agenten (ModelFactory / OPENAI_*). Nach dem
          Speichern erscheint der Workflow unter Workflows &amp; Agents.
        </p>
        @if (info(); as i) {
          <p class="small" style="color: var(--ok)">{{ i }}</p>
        }
        @if (lastRunId(); as rid) {
          <p class="small">
            Lauf gestartet:
            <a class="mono" [routerLink]="['/runs', rid]">{{ rid }}</a>
          </p>
        }
      </div>

      <div>
        @if (doc(); as d) {
          <div class="grid cols-4" style="margin-bottom: 12px">
            <div class="card stat"><span class="label">Nodes</span><span class="value">{{ nodes().length }}</span></div>
            <div class="card stat"><span class="label">Agents</span><span class="value">{{ agents().length }}</span></div>
            <div class="card stat"><span class="label">Status</span><span class="value small">{{ meta('status') }}</span></div>
            <div class="card stat"><span class="label">ID</span><span class="value small mono">{{ meta('id') }}</span></div>
          </div>

          <div class="card" style="margin-bottom: 12px">
            <div class="toolbar">
              <a class="btn small" [routerLink]="['/workflows', meta('id')]">Workflow öffnen →</a>
            </div>
            <table>
              <thead>
                <tr><th>ID</th><th>Type</th><th>Titel</th><th>DependsOn</th><th>Agent</th></tr>
              </thead>
              <tbody>
                @for (n of nodes(); track n['id']) {
                  <tr>
                    <td class="mono">{{ n['id'] }}</td>
                    <td><span class="badge">{{ n['type'] }}</span></td>
                    <td>{{ n['title'] }}</td>
                    <td class="mono small">{{ join(n['dependsOn']) }}</td>
                    <td class="mono small">{{ agentRef(n) }}</td>
                  </tr>
                }
              </tbody>
            </table>
          </div>

          <div class="card" style="margin-bottom: 12px">
            <h2>Agents</h2>
            @for (a of agents(); track a['id']) {
              <span class="chip">{{ a['name'] }} <span class="mono dim">({{ a['id'] }})</span></span>
            } @empty {
              <div class="empty">Keine Agents.</div>
            }
          </div>

          <div class="card">
            <h2>JSON</h2>
            <pre class="pre">{{ json() }}</pre>
          </div>
        } @else {
          <div class="empty">Noch keine Vorschau. Aufgabe eingeben und „Vorschau“ klicken.</div>
        }
      </div>
    </div>
  `,
})
export class GeneratePage {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);

  task = '';
  name = '';

  readonly doc = signal<WorkflowDoc | null>(null);
  readonly busy = signal(false);
  readonly info = signal('');
  readonly lastRunId = signal('');

  /// ── actions ──────────────────────────────────────────────────────────────
  preview(): void {
    if (!this.task.trim()) {
      this.toast.error('Bitte eine Aufgabe eingeben');
      return;
    }
    this.busy.set(true);
    this.info.set('');
    this.lastRunId.set('');
    this.api.generateWorkflow(this.task, this.name || undefined).subscribe({
      next: (r) => {
        this.busy.set(false);
        this.doc.set((r['workflow'] as WorkflowDoc) ?? null);
        this.toast.success('Workflow generiert (Vorschau, nicht gespeichert)');
      },
      error: (err) => {
        this.busy.set(false);
        this.toast.error('Generieren fehlgeschlagen: ' + this.msg(err));
      },
    });
  }

  save(): void {
    if (!this.doc()) return;
    this.busy.set(true);
    this.api.saveGeneratedWorkflow(this.task, this.name || undefined).subscribe({
      next: (s) => {
        this.busy.set(false);
        this.info.set(`Gespeichert als '${s.id}' (${s.nodeCount} Nodes, ${s.agentRefs.length} Agents).`);
        this.toast.success('Gespeichert (Runtime-Override + Datei)');
      },
      error: (err) => {
        this.busy.set(false);
        this.toast.error('Speichern fehlgeschlagen: ' + this.msg(err));
      },
    });
  }

  run(): void {
    if (!this.doc()) return;
    this.busy.set(true);
    this.api.runGeneratedWorkflow(this.task, this.name || undefined).subscribe({
      next: (r) => {
        this.busy.set(false);
        const rid = String(r['workflowId'] ?? '');
        this.lastRunId.set(rid);
        this.info.set(`Lauf gestartet (Definition '${r['definitionId']}').`);
        this.toast.success('Durabler Lauf gestartet');
      },
      error: (err) => {
        this.busy.set(false);
        this.toast.error('Ausführen fehlgeschlagen: ' + this.msg(err));
      },
    });
  }

  /// ── view helpers ─────────────────────────────────────────────────────────
  private main(): Record<string, unknown> {
    return (this.doc()?.['mainWorkflow'] as Record<string, unknown>) ?? {};
  }

  meta(key: string): string {
    return String(this.main()[key] ?? '');
  }

  nodes(): Record<string, unknown>[] {
    return (this.main()['nodes'] as Record<string, unknown>[]) ?? [];
  }

  agents(): Record<string, unknown>[] {
    return (this.doc()?.['agents'] as Record<string, unknown>[]) ?? [];
  }

  agentRef(node: Record<string, unknown>): string {
    const ref = node['agent'] as { ref?: string } | undefined;
    return ref?.ref ?? '—';
  }

  join(value: unknown): string {
    return Array.isArray(value) ? value.join(', ') : '—';
  }

  json(): string {
    return JSON.stringify(this.doc() ?? {}, null, 2);
  }

  private msg(err: unknown): string {
    const e = err as { error?: { message?: string }; message?: string };
    return e?.error?.message ?? e?.message ?? String(err);
  }
}
