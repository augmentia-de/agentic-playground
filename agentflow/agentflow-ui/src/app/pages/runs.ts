import { Component, DestroyRef, OnInit, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { ApiService } from '../core/api.service';
import { RunSummary } from '../core/models';
import { RunStreamService } from '../core/run-stream.service';
import { isRunning, statusClass, timeAgo } from '../core/util';
import { ToastService } from '../core/toast.service';
import { StartForm } from '../ui/start-form';

@Component({
  selector: 'af-runs',
  imports: [RouterLink, FormsModule, StartForm],
  template: `
    <div class="page-head">
      <div>
        <h1>Runs</h1>
        <p class="subtitle">Temporal-Visibility: Liste, Status, Detail, Signale.</p>
      </div>
      <span class="small dim">{{ live() ? '● live (SSE)' : '○ polling' }}</span>
    </div>

    <div class="grid cols-2">
      <div>
        <div class="toolbar">
          <input
            style="max-width: 320px"
            placeholder='Query, z. B. ExecutionStatus = "Running"'
            [(ngModel)]="query"
            [ngModelOptions]="{ standalone: true }"
            (change)="connect()"
            (keyup.enter)="connect()"
          />
          <input style="max-width: 90px" type="number" [(ngModel)]="limit" [ngModelOptions]="{ standalone: true }" />
          <button class="primary" (click)="connect()">Anwenden</button>
        </div>

        <div class="card">
          @if (error()) {
            <div class="empty">Temporal nicht erreichbar: {{ error() }}</div>
          } @else if (runs().length) {
            <table>
              <thead>
                <tr><th>Workflow</th><th>Typ</th><th>Status</th><th>Start</th><th></th></tr>
              </thead>
              <tbody>
                @for (r of runs(); track r.workflowId) {
                  <tr>
                    <td class="mono"><a [routerLink]="['/runs', r.workflowId]">{{ r.workflowId }}</a></td>
                    <td>{{ r.workflowType }}</td>
                    <td><span [class]="badge(r.status)">{{ r.status }}</span></td>
                    <td class="dim small">{{ ago(r.startTime) }}</td>
                    <td>
                      @if (running(r.status)) {
                        <button class="small" (click)="terminate(r)">Terminate</button>
                      }
                    </td>
                  </tr>
                }
              </tbody>
            </table>
          } @else {
            <div class="empty">Keine Runs gefunden.</div>
          }
        </div>
      </div>

      <div>
        <af-start-form></af-start-form>
      </div>
    </div>
  `,
})
export class RunsPage implements OnInit {
  private readonly api = inject(ApiService);
  private readonly stream = inject(RunStreamService);
  private readonly toast = inject(ToastService);
  private readonly destroyRef = inject(DestroyRef);

  query = '';
  limit = 50;

  readonly runs = signal<RunSummary[]>([]);
  readonly error = signal('');
  readonly live = signal(false);
  readonly badge = statusClass;
  readonly ago = timeAgo;
  readonly running = isRunning;

  private stop: (() => void) | null = null;

  ngOnInit(): void {
    this.connect();
    this.destroyRef.onDestroy(() => this.stop?.());
  }

  connect(): void {
    this.stop?.();
    this.error.set('');
    this.stop = this.stream.streamList(
      this.query,
      this.limit,
      (runs) => {
        this.runs.set(runs);
        this.live.set(true);
        this.error.set('');
      },
      () => {
        // SSE unavailable → fall back to a one-shot REST read.
        this.live.set(false);
        this.load();
      },
    );
  }

  load(): void {
    this.api.runs(this.query, this.limit).subscribe({
      next: (r) => {
        this.runs.set(r);
        this.error.set('');
      },
      error: (err) => this.error.set(err?.message ?? String(err?.status ?? err)),
    });
  }

  terminate(run: RunSummary): void {
    if (!confirm(`Run ${run.workflowId} wirklich terminieren?`)) {
      return;
    }
    this.api.terminate(run.workflowId, 'terminated via UI').subscribe({
      next: () => {
        this.toast.success('Terminiert: ' + run.workflowId);
        this.load();
      },
      error: (err) => this.toast.error('Terminate fehlgeschlagen: ' + (err?.message ?? err)),
    });
  }
}
