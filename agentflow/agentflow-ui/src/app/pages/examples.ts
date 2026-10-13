import { Component, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../core/api.service';
import { ExampleCase } from '../core/models';
import { ToastService } from '../core/toast.service';

const FIELDS: Record<string, { name: string; label: string; type: string; value: string | number }[]> = {
  'support-triage': [
    { name: 'customerId', label: 'Customer ID', type: 'text', value: 'c-1' },
    { name: 'message', label: 'Message', type: 'textarea', value: 'Meine Rechnung ist falsch' },
  ],
  'contract-review': [
    { name: 'contract', label: 'Contract text', type: 'textarea', value: 'The supplier may terminate at any time.' },
  ],
  'invoice-processing': [
    { name: 'invoiceId', label: 'Invoice ID', type: 'text', value: 'inv-1' },
    { name: 'amount', label: 'Amount (>100 → compensation)', type: 'number', value: 500 },
  ],
  'market-research': [
    { name: 'topic', label: 'Topic', type: 'textarea', value: 'Cargo bikes in European cities' },
  ],
  'onboarding': [
    { name: 'newHire', label: 'New hire', type: 'text', value: 'Ada' },
    { name: 'projectScope', label: 'Project scope', type: 'text', value: 'agentflow' },
  ],
};

@Component({
  selector: 'af-examples',
  imports: [FormsModule],
  template: `
    <div class="page-head">
      <div>
        <h1>Examples</h1>
        <p class="subtitle">Fünf Business-Cases als durable Workflows.</p>
      </div>
    </div>

    <div class="grid cols-2">
      @for (c of cases(); track c.id) {
        <div class="card">
          <h3>{{ c.title }}</h3>
          <p class="small dim mono">{{ c.endpoint }}</p>
          <p class="small dim">{{ c.capabilities }}</p>

          @for (f of fieldsFor(c.id); track f.name) {
            <div class="field">
              <label>{{ f.label }}</label>
              @if (f.type === 'textarea') {
                <textarea [(ngModel)]="values[c.id][f.name]" [ngModelOptions]="{ standalone: true }"></textarea>
              } @else {
                <input [type]="f.type" [(ngModel)]="values[c.id][f.name]" [ngModelOptions]="{ standalone: true }" />
              }
            </div>
          }
          <button class="primary" (click)="start(c)">Starten</button>
        </div>
      } @empty {
        <div class="empty">Keine Cases geladen.</div>
      }
    </div>
  `,
})
export class ExamplesPage {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);

  readonly cases = signal<ExampleCase[]>([]);
  values: Record<string, Record<string, string | number>> = {};

  constructor() {
    this.api.examples().subscribe({
      next: (cases) => {
        this.cases.set(cases);
        for (const c of cases) {
          this.values[c.id] = {};
          for (const f of this.fieldsFor(c.id)) {
            this.values[c.id][f.name] = f.value;
          }
        }
      },
      error: (err) => this.toast.error('Laden fehlgeschlagen: ' + (err?.message ?? err)),
    });
  }

  fieldsFor(id: string): { name: string; label: string; type: string; value: string | number }[] {
    return FIELDS[id] ?? [{ name: 'input', label: 'Input', type: 'textarea', value: '' }];
  }

  start(c: ExampleCase): void {
    const path = c.endpoint.replace(/^\s*(GET|POST|PUT)\s+/, '');
    this.api.start(path, this.values[c.id] ?? {}).subscribe({
      next: (r) => this.toast.success(`Gestartet: ${r['workflowId'] ?? c.id}`),
      error: (err) => this.toast.error('Start fehlgeschlagen: ' + (err?.message ?? err)),
    });
  }
}
