import { JsonPipe } from '@angular/common';
import { Component, DestroyRef, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { RouterLink } from '@angular/router';
import { timer } from 'rxjs';
import { ApiService } from '../core/api.service';
import { RunSummary } from '../core/models';
import { parseAgentMetrics, statusClass, timeAgo } from '../core/util';
import { StartForm } from '../ui/start-form';

@Component({
  selector: 'af-dashboard',
  imports: [RouterLink, StartForm, JsonPipe],
  template: `
    <div class="page-head">
      <div>
        <h1>Dashboard</h1>
        <p class="subtitle">Lagebild, Metriken und Schnellstart.</p>
      </div>
    </div>

    <div class="grid cols-4">
      <div class="card stat">
        <span class="label">Backend</span>
        <span class="value"><span [class]="'dot ' + health()"></span>{{ health() }}</span>
      </div>
      <div class="card stat">
        <span class="label">Workflow-Definitionen</span>
        <span class="value">{{ workflowCount() }}</span>
      </div>
      <div class="card stat">
        <span class="label">Capabilities</span>
        <span class="value">{{ capabilityCount() }}</span>
      </div>
      <div class="card stat">
        <span class="label">Aktive Runs</span>
        <span class="value">{{ recent().length }}</span>
      </div>
    </div>

    <div class="grid cols-2" style="margin-top: 14px">
      <div>
        <h2>Metriken (Prometheus)</h2>
        <div class="card">
          @if (metrics().length) {
            <table>
              <thead>
                <tr><th>Metrik</th><th>Wert</th></tr>
              </thead>
              <tbody>
                @for (m of metrics(); track m.name) {
                  <tr>
                    <td class="mono">{{ m.name }}</td>
                    <td class="mono">{{ m.value }}</td>
                  </tr>
                }
              </tbody>
            </table>
          } @else {
            <div class="empty">Keine agent_*-Metriken (noch kein Run ausgeführt?).</div>
          }
        </div>

        <h2>Letzte Runs</h2>
        <div class="card">
          @if (recent().length) {
            <table>
              <thead>
                <tr><th>Workflow</th><th>Typ</th><th>Status</th><th>Start</th></tr>
              </thead>
              <tbody>
                @for (r of recent(); track r.workflowId) {
                  <tr>
                    <td class="mono"><a [routerLink]="['/runs', r.workflowId]">{{ r.workflowId }}</a></td>
                    <td>{{ r.workflowType }}</td>
                    <td><span [class]="badge(r.status)">{{ r.status }}</span></td>
                    <td class="dim small">{{ ago(r.startTime) }}</td>
                  </tr>
                }
              </tbody>
            </table>
          } @else {
            <div class="empty">Keine Runs. Starte rechts einen Lauf.</div>
          }
        </div>

        <h2>Engine</h2>
        <div class="card">
          <pre class="pre">{{ info() | json }}</pre>
        </div>
      </div>

      <div>
        <af-start-form [engineKey]="'quad'"></af-start-form>
      </div>
    </div>
  `,
})
export class DashboardPage {
  private readonly api = inject(ApiService);
  private readonly destroyRef = inject(DestroyRef);

  readonly health = signal<'up' | 'down' | 'unknown'>('unknown');
  readonly workflowCount = signal(0);
  readonly capabilityCount = signal(0);
  readonly recent = signal<RunSummary[]>([]);
  readonly metrics = signal<{ name: string; value: string }[]>([]);
  readonly info = signal<Record<string, unknown>>({});

  readonly badge = statusClass;
  readonly ago = timeAgo;

  constructor() {
    timer(0, 8000)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(() => {
        this.api.health().subscribe({
          next: (h) => this.health.set(h.status === 'UP' ? 'up' : 'down'),
          error: () => this.health.set('down'),
        });
        this.api.workflows().subscribe({ next: (w) => this.workflowCount.set(w.length), error: () => {} });
        this.api.capabilities().subscribe({ next: (c) => this.capabilityCount.set(c.length), error: () => {} });
        this.api.runs('', 5).subscribe({ next: (r) => this.recent.set(r), error: () => {} });
        this.api.info().subscribe({ next: (i) => this.info.set(i), error: () => {} });
        this.api.prometheus().subscribe({
          next: (text) => this.metrics.set(parseAgentMetrics(text).slice(0, 12)),
          error: () => this.metrics.set([]),
        });
      });
  }
}
