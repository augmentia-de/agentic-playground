import { Component, computed, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../core/api.service';
import { Settings } from '../core/models';
import { ToastService } from '../core/toast.service';

interface Section {
  title: string;
  rows: [string, string][];
}

@Component({
  selector: 'af-settings',
  imports: [FormsModule],
  template: `
    <div class="page-head">
      <div>
        <h1>Settings</h1>
        <p class="subtitle">Effektive Konfiguration (read-only, ohne Secrets) + Client-Header.</p>
      </div>
    </div>

    <div class="grid cols-2">
      @for (s of sections(); track s.title) {
        <div class="card">
          <h3>{{ s.title }}</h3>
          <table>
            <tbody>
              @for (r of s.rows; track r[0]) {
                <tr>
                  <td class="dim">{{ r[0] }}</td>
                  <td class="mono">{{ r[1] }}</td>
                </tr>
              }
            </tbody>
          </table>
        </div>
      } @empty {
        <div class="empty">Lade Settings…</div>
      }

      <div class="card">
        <h3>Client (dieser Browser)</h3>
        <p class="small dim">
          Optional: API-Key/Header für die opt-in Sicherheits- und Tenant-Features des Backends.
          Wird nur lokal (localStorage) gespeichert und an Requests angehängt.
        </p>
        <div class="field"><label>X-API-Key</label><input [(ngModel)]="apiKey" [ngModelOptions]="{ standalone: true }" /></div>
        <div class="field"><label>X-Tenant-Id</label><input [(ngModel)]="tenant" [ngModelOptions]="{ standalone: true }" /></div>
        <div class="toolbar">
          <button class="primary" (click)="saveClient()">Speichern</button>
          <button class="ghost" (click)="clearClient()">Leeren</button>
        </div>
      </div>
    </div>
  `,
})
export class SettingsPage {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);

  readonly settings = signal<Settings | null>(null);

  apiKey = localStorage.getItem('agentflow.apiKey') ?? '';
  tenant = localStorage.getItem('agentflow.tenant') ?? '';

  readonly sections = computed<Section[]>(() => {
    const s = this.settings();
    if (!s) {
      return [];
    }
    return [
      {
        title: 'LLM (OpenAI-kompatibel)',
        rows: [
          ['Model', s.llm.model],
          ['Base URL', s.llm.baseUrl],
          ['API-Key gesetzt', s.llm.hasApiKey ? 'ja' : 'nein'],
          ['Headers konfiguriert', s.llm.headersConfigured ? 'ja' : 'nein'],
          ['Header-Namen', (s.llm.headerNames ?? []).join(', ') || '–'],
        ],
      },
      {
        title: 'Durability (Temporal)',
        rows: [
          ['Target', s.temporal.target],
          ['Namespace', s.temporal.namespace],
          ['Task Queue', s.temporal.taskQueue],
        ],
      },
      {
        title: 'Capability-Search',
        rows: [
          ['Index', s.capability.index],
          ['Table', s.capability.table],
          ['Autoload', String(s.capability.autoload)],
        ],
      },
      {
        title: 'Workflows & Checkpoints',
        rows: [
          ['Workflow location', s.workflows.location],
          ['State dir', s.workflows.stateDir ?? '–'],
          ['Checkpoint path', s.checkpoint.path],
        ],
      },
      {
        title: 'Endpoints',
        rows: [
          ['Health', s.endpoints.health],
          ['Metrics', s.endpoints.metrics],
          ['Temporal UI', s.endpoints.temporalUi],
        ],
      },
    ];
  });

  constructor() {
    this.api.settings().subscribe({
      next: (s) => this.settings.set(s),
      error: (err) => this.toast.error('Settings laden fehlgeschlagen: ' + (err?.message ?? err)),
    });
  }

  saveClient(): void {
    localStorage.setItem('agentflow.apiKey', this.apiKey.trim());
    localStorage.setItem('agentflow.tenant', this.tenant.trim());
    this.toast.success('Client-Header gespeichert');
  }

  clearClient(): void {
    this.apiKey = '';
    this.tenant = '';
    localStorage.removeItem('agentflow.apiKey');
    localStorage.removeItem('agentflow.tenant');
    this.toast.info('Client-Header geleert');
  }
}
