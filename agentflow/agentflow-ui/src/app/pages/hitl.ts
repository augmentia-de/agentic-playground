import { Component, DestroyRef, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { RouterLink } from '@angular/router';
import { timer } from 'rxjs';
import { ApiService } from '../core/api.service';
import { RunSummary } from '../core/models';
import { statusClass, timeAgo } from '../core/util';
import { ToastService } from '../core/toast.service';

const HITL_TYPES = [
  'HitlWorkflow',
  'DeclarativeAgentWorkflow',
  'SupportTriageWorkflow',
  'ContractReviewWorkflow',
  'AuthorizedAgentWorkflow',
];

@Component({
  selector: 'af-hitl',
  imports: [RouterLink],
  template: `
    <div class="page-head">
      <div>
        <h1>Human-in-the-Loop</h1>
        <p class="subtitle">Wartende Runs (Status RUNNING) mit Entscheidungsbedarf.</p>
      </div>
      <button class="small" (click)="load()">Aktualisieren</button>
    </div>

    @if (waiting().length) {
      <div class="grid cols-2">
        @for (r of waiting(); track r.workflowId) {
          <div class="card">
            <div class="row">
              <strong class="mono">{{ r.workflowId }}</strong>
              <span class="badge warn right">{{ r.workflowType }}</span>
            </div>
            <p class="small dim">wartet seit {{ ago(r.startTime) }} · {{ r.startTime }}</p>
            <div class="toolbar">
              <button class="primary" (click)="decide(r, true)">Approve / Escalate</button>
              <button class="danger" (click)="decide(r, false)">Reject</button>
              <a class="btn small" [routerLink]="['/runs', r.workflowId]">Detail</a>
            </div>
          </div>
        }
      </div>
    } @else {
      <div class="empty">Keine wartenden Runs. (Auto-Refresh 5s)</div>
    }
  `,
})
export class HitlPage {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);
  private readonly destroyRef = inject(DestroyRef);

  readonly waiting = signal<RunSummary[]>([]);
  readonly ago = timeAgo;
  readonly badge = statusClass;

  constructor() {
    timer(0, 5000)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(() => this.load());
  }

  load(): void {
    this.api.runs('ExecutionStatus = "Running"', 200).subscribe({
      next: (runs) => this.waiting.set(runs.filter((r) => HITL_TYPES.includes(r.workflowType ?? ''))),
      error: () => this.waiting.set([]),
    });
  }

  decide(run: RunSummary, approved: boolean): void {
    const call =
      run.workflowType === 'HitlWorkflow'
        ? this.api.hitlDecide(run.workflowId, approved)
        : run.workflowType === 'SupportTriageWorkflow'
          ? this.api.escalate(run.workflowId, approved)
          : this.api.declarativeApprove(run.workflowId, approved);
    call.subscribe({
      next: () => {
        this.toast.success('Signal gesendet: ' + run.workflowId);
        setTimeout(() => this.load(), 800);
      },
      error: (err) => this.toast.error('Signal fehlgeschlagen: ' + (err?.message ?? err)),
    });
  }
}
