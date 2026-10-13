import { Component, DestroyRef, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormsModule } from '@angular/forms';
import { timer } from 'rxjs';
import { ApiService } from '../core/api.service';
import { Capability } from '../core/models';
import { parseAgentMetrics } from '../core/util';
import { ToastService } from '../core/toast.service';

@Component({
  selector: 'af-observe',
  imports: [FormsModule],
  template: `
    <div class="page-head">
      <div>
        <h1>Observability</h1>
        <p class="subtitle">Metriken (Micrometer/Prometheus), Provenance, Capabilities, Rollen.</p>
      </div>
      <a class="btn small" [href]="temporalUi()" target="_blank" rel="noopener">Temporal UI ↗</a>
    </div>

    <div class="grid cols-2">
      <div>
        <h2>Agent-Metriken</h2>
        <div class="card">
          @if (metrics().length) {
            <table>
              <thead><tr><th>Metrik</th><th>Wert</th></tr></thead>
              <tbody>
                @for (m of metrics(); track m.name) {
                  <tr><td class="mono">{{ m.name }}</td><td class="mono">{{ m.value }}</td></tr>
                }
              </tbody>
            </table>
          } @else {
            <div class="empty">Keine agent_*-Metriken.</div>
          }
          <div class="toolbar" style="margin-top: 10px">
            <button class="small ghost" (click)="showRaw.set(!showRaw())">
              {{ showRaw() ? 'Roh-Prometheus ausblenden' : 'Roh-Prometheus anzeigen' }}
            </button>
          </div>
          @if (showRaw()) {
            <pre class="pre">{{ raw().slice(0, 4000) }}</pre>
          }
        </div>

        <h2>Provenance</h2>
        <div class="card">
          <div class="field">
            <label>Prompt</label>
            <input [(ngModel)]="provInput" [ngModelOptions]="{ standalone: true }" />
          </div>
          <button class="primary" (click)="runProvenance()">Provenance-Run starten</button>
          <p class="small dim">Ergebnis erscheint nach Abschluss im Run-Detail (ProvenanceTracker als JSON).</p>
        </div>
      </div>

      <div>
        <h2>Capabilities ({{ capabilities().length }})</h2>
        <div class="card">
          <div class="toolbar">
            <input placeholder="Suche…" [(ngModel)]="capQuery" [ngModelOptions]="{ standalone: true }" (keyup.enter)="searchCaps()" />
            <button (click)="searchCaps()">Suchen</button>
            <button class="ghost" (click)="resetCaps()">Alle</button>
          </div>
          <table>
            <thead><tr><th>Name</th><th>Typ</th><th>Beschreibung</th></tr></thead>
            <tbody>
              @for (c of capabilities(); track c.name) {
                <tr><td class="mono">{{ c.name }}</td><td>{{ c.type }}</td><td class="small dim">{{ c.description }}</td></tr>
              }
            </tbody>
          </table>
        </div>

        <h2>Rollen (RBAC)</h2>
        <div class="card">
          @for (r of roles(); track r) {
            <span class="chip">{{ r }}</span>
          }
        </div>
      </div>
    </div>
  `,
})
export class ObservePage {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);
  private readonly destroyRef = inject(DestroyRef);

  readonly metrics = signal<{ name: string; value: string }[]>([]);
  readonly raw = signal('');
  readonly showRaw = signal(false);
  readonly capabilities = signal<Capability[]>([]);
  readonly roles = signal<string[]>([]);
  readonly temporalUi = signal('http://localhost:8233');

  provInput = 'What time is it?';
  capQuery = '';

  constructor() {
    timer(0, 15000)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(() => {
        this.api.prometheus().subscribe({
          next: (text) => {
            this.raw.set(text);
            this.metrics.set(parseAgentMetrics(text).slice(0, 30));
          },
          error: () => {},
        });
      });
    this.resetCaps();
    this.api.roles().subscribe({
      next: (r) => this.roles.set(((r['roles'] as string[]) ?? []).length ? (r['roles'] as string[]) : Object.keys(r)),
      error: () => {},
    });
    this.api.settings().subscribe({ next: (s) => this.temporalUi.set(s.endpoints.temporalUi), error: () => {} });
  }

  resetCaps(): void {
    this.capQuery = '';
    this.api.capabilities().subscribe({ next: (c) => this.capabilities.set(c), error: () => {} });
  }

  searchCaps(): void {
    if (!this.capQuery.trim()) {
      this.resetCaps();
      return;
    }
    this.api.searchCapabilities(this.capQuery, 20).subscribe({
      next: (c) => this.capabilities.set(c),
      error: (err) => this.toast.error('Suche fehlgeschlagen: ' + (err?.message ?? err)),
    });
  }

  runProvenance(): void {
    this.api.start('/agentflow/runs/provenance', { input: this.provInput }).subscribe({
      next: (r) => this.toast.success('Provenance-Run gestartet: ' + (r['workflowId'] ?? '')),
      error: (err) => this.toast.error('Start fehlgeschlagen: ' + (err?.message ?? err)),
    });
  }
}
