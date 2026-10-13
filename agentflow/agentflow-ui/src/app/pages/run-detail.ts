import { Component, DestroyRef, OnInit, inject, input, signal } from '@angular/core';
import { RouterLink } from '@angular/router';
import { ApiService } from '../core/api.service';
import { RunDetail, RunEvent } from '../core/models';
import { RunStreamService } from '../core/run-stream.service';
import { isRunning, pretty, statusClass } from '../core/util';
import { ToastService } from '../core/toast.service';

@Component({
  selector: 'af-run-detail',
  imports: [RouterLink],
  template: `
    <div class="page-head">
      <div>
        <h1>Run</h1>
        <p class="subtitle mono">{{ id() }} <span class="small dim">{{ live() ? '● live' : '' }}</span></p>
      </div>
      <a class="btn small" routerLink="/runs">← zur Liste</a>
    </div>

    @if (run(); as r) {
      <div class="grid cols-4">
        <div class="card stat"><span class="label">Status</span><span class="value"><span [class]="badge(r.status)">{{ r.status }}</span></span></div>
        <div class="card stat"><span class="label">Typ</span><span class="value small">{{ r.workflowType }}</span></div>
        <div class="card stat"><span class="label">History</span><span class="value">{{ r.historyLength }}</span></div>
        <div class="card stat"><span class="label">Start</span><span class="value small">{{ r.startTime }}</span></div>
      </div>

      <div class="card" style="margin-top: 14px">
        <div class="toolbar">
          @if (canDecide()) {
            <button class="primary" (click)="decide(true)">Approve / Escalate</button>
            <button class="danger" (click)="decide(false)">Reject</button>
          }
          @if (running(r.status)) {
            <button class="right" (click)="terminate()">Terminate</button>
          }
        </div>
        @if (!canDecide()) {
          <p class="small dim">Für diesen Workflow-Typ sind keine Signal-Aktionen hinterlegt.</p>
        }
      </div>

      <h2>Ergebnis</h2>
      <div class="card">
        @if (r.result) {
          <pre class="pre">{{ format(r.result) }}</pre>
        } @else {
          <div class="empty">{{ r.status === 'RUNNING' ? 'Noch nicht abgeschlossen (wartet ggf. auf Signal).' : 'Kein Ergebnis-Payload.' }}</div>
        }
      </div>

      <h2>Log (History-Events)</h2>
      <div class="card">
        @if (events().length) {
          <table>
            <thead>
              <tr><th>#</th><th>Event</th><th>Detail</th><th>Zeit</th></tr>
            </thead>
            <tbody>
              @for (e of events(); track e.eventId) {
                <tr>
                  <td class="mono">{{ e.eventId }}</td>
                  <td>{{ e.type }}</td>
                  <td class="small dim">{{ e.detail }}</td>
                  <td class="mono small">{{ e.time }}</td>
                </tr>
              }
            </tbody>
          </table>
        } @else {
          <div class="empty">Keine Events geladen.{{ live() ? ' (live…)' : '' }}</div>
        }
      </div>

      <h2>Rohdaten</h2>
      <div class="card"><pre class="pre">{{ format(r) }}</pre></div>
    } @else {
      <div class="empty">Lade Run…</div>
    }
  `,
})
export class RunDetailPage implements OnInit {
  private readonly api = inject(ApiService);
  private readonly stream = inject(RunStreamService);
  private readonly toast = inject(ToastService);
  private readonly destroyRef = inject(DestroyRef);

  readonly id = input<string>();

  readonly run = signal<RunDetail | null>(null);
  readonly events = signal<RunEvent[]>([]);
  readonly live = signal(false);
  readonly badge = statusClass;
  readonly format = pretty;
  readonly running = isRunning;

  private stop: (() => void) | null = null;

  ngOnInit(): void {
    const id = this.id();
    if (!id) {
      return;
    }
    this.load();
    this.loadEvents();
    this.stop = this.stream.streamRun(
      id,
      (r) => {
        this.run.set(r);
        this.live.set(r.status === 'RUNNING');
      },
      (fresh) => this.mergeEvents(fresh),
      () => this.live.set(false),
    );
    this.destroyRef.onDestroy(() => this.stop?.());
  }

  private mergeEvents(fresh: RunEvent[]): void {
    if (!fresh.length) {
      return;
    }
    const byId = new Map(this.events().map((e) => [e.eventId, e]));
    for (const e of fresh) {
      byId.set(e.eventId, e);
    }
    this.events.set([...byId.values()].sort((a, b) => a.eventId - b.eventId));
  }

  loadEvents(): void {
    const id = this.id();
    if (!id) {
      return;
    }
    this.api.runEvents(id, 500).subscribe({
      next: (e) => this.mergeEvents(e),
      error: () => {},
    });
  }

  load(): void {
    const id = this.id();
    if (!id) {
      return;
    }
    this.api.run(id).subscribe({
      next: (r) => this.run.set(r),
      error: () => this.run.set(null),
    });
  }

  canDecide(): boolean {
    const type = this.run()?.workflowType ?? '';
    return [
      'HitlWorkflow',
      'DeclarativeAgentWorkflow',
      'SupportTriageWorkflow',
      'ContractReviewWorkflow',
      'AuthorizedAgentWorkflow',
    ].includes(type);
  }

  decide(approved: boolean): void {
    const r = this.run();
    if (!r) {
      return;
    }
    const type = r.workflowType ?? '';
    const call =
      type === 'HitlWorkflow'
        ? this.api.hitlDecide(r.workflowId, approved)
        : type === 'SupportTriageWorkflow'
          ? this.api.escalate(r.workflowId, approved)
          : this.api.declarativeApprove(r.workflowId, approved);
    call.subscribe({
      next: () => {
        this.toast.success('Signal gesendet: approved=' + approved);
        setTimeout(() => this.load(), 800);
      },
      error: (err) => this.toast.error('Signal fehlgeschlagen: ' + (err?.message ?? err)),
    });
  }

  terminate(): void {
    const r = this.run();
    if (!r || !confirm(`Run ${r.workflowId} terminieren?`)) {
      return;
    }
    this.api.terminate(r.workflowId).subscribe({
      next: () => {
        this.toast.success('Terminiert');
        this.load();
      },
      error: (err) => this.toast.error('Terminate fehlgeschlagen: ' + (err?.message ?? err)),
    });
  }
}
