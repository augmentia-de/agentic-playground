import { Component, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router, RouterLink } from '@angular/router';
import { ApiService } from '../core/api.service';
import { WorkflowDefinitionSummary } from '../core/models';
import { ToastService } from '../core/toast.service';

@Component({
  selector: 'af-workflows',
  imports: [FormsModule, RouterLink],
  template: `
    <div class="page-head">
      <div>
        <h1>Workflows</h1>
        <p class="subtitle">Deklarative quad.workflow-v2-Definitionen (validiert beim Start).</p>
      </div>
      <div class="toolbar">
        <input [(ngModel)]="newName" [ngModelOptions]="{ standalone: true }" placeholder="Name des neuen Workflows" style="max-width: 240px" />
        <button class="primary" (click)="create()" [disabled]="busy()">+ Neuer Workflow</button>
        <a class="btn small" routerLink="/runs">Ausführen unter Runs →</a>
      </div>
    </div>

    <div class="card">
      @if (definitions().length) {
        <table>
          <thead>
            <tr><th>ID</th><th>Name</th><th>Version</th><th>Status</th><th>Nodes</th><th>Agents</th></tr>
          </thead>
          <tbody>
            @for (d of definitions(); track d.id) {
              <tr>
                <td class="mono"><a [routerLink]="['/workflows', d.id]">{{ d.id }}</a></td>
                <td>{{ d.name }}</td>
                <td>{{ d.version }}</td>
                <td><span class="badge">{{ d.status }}</span></td>
                <td>{{ d.nodeCount }}</td>
                <td>
                  @for (a of d.agentRefs; track a) {
                    <span class="chip">{{ a }}</span>
                  }
                </td>
              </tr>
            }
          </tbody>
        </table>
      } @else {
        <div class="empty">Keine Definitionen geladen.</div>
      }
    </div>
  `,
})
export class WorkflowsPage {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);
  private readonly router = inject(Router);

  readonly definitions = signal<WorkflowDefinitionSummary[]>([]);
  readonly busy = signal(false);
  newName = '';

  constructor() {
    this.load();
  }

  private load(): void {
    this.api.workflows().subscribe({
      next: (d) => this.definitions.set(d),
      error: (err) => this.toast.error('Laden fehlgeschlagen: ' + (err?.message ?? err)),
    });
  }

  /** Creates an empty v2 workflow (single end node) and opens it for editing. */
  create(): void {
    this.busy.set(true);
    this.api.createWorkflow(this.newName.trim() || undefined).subscribe({
      next: (summary) => {
        this.busy.set(false);
        this.newName = '';
        this.toast.success('Workflow angelegt: ' + summary.id);
        this.router.navigate(['/workflows', summary.id]);
      },
      error: (err) => {
        this.busy.set(false);
        this.toast.error('Anlegen fehlgeschlagen: ' + (err?.error?.message ?? err?.message ?? err));
      },
    });
  }
}
