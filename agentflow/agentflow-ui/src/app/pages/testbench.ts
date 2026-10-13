import { Component, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../core/api.service';
import { ToastService } from '../core/toast.service';

@Component({
  selector: 'af-testbench',
  imports: [FormsModule],
  template: `
    <div class="page-head">
      <div>
        <h1>Agent-Testbench</h1>
        <p class="subtitle">Einzelnen Agenten ausführen und Tokens, Tools, Kosten &amp; Provenance sehen.</p>
      </div>
    </div>

    <div class="grid cols-2">
      <div class="card">
        <div class="field"><label>System Prompt</label><textarea [(ngModel)]="systemPrompt" [ngModelOptions]="{ standalone: true }"></textarea></div>
        <div class="field"><label>Prompt</label><textarea [(ngModel)]="prompt" [ngModelOptions]="{ standalone: true }"></textarea></div>
        <div class="field"><label>Tools (kommagetrennt)</label><input [(ngModel)]="tools" [ngModelOptions]="{ standalone: true }" placeholder="readFile, webSearch" /></div>
        <button class="primary" (click)="run()" [disabled]="busy()">Ausführen</button>
        <p class="small dim">Kosten = Tokens × konfigurierte Preise (agentflow.testbench.*-price-per-1k).</p>
      </div>

      <div>
        @if (result(); as r) {
          <div class="grid cols-4" style="margin-bottom: 12px">
            <div class="card stat"><span class="label">Input tokens</span><span class="value">{{ r['inputTokens'] }}</span></div>
            <div class="card stat"><span class="label">Output tokens</span><span class="value">{{ r['outputTokens'] }}</span></div>
            <div class="card stat"><span class="label">Tool calls</span><span class="value">{{ r['toolCalls'] }}</span></div>
            <div class="card stat"><span class="label">Dauer (ms)</span><span class="value">{{ r['durationMs'] }}</span></div>
          </div>
          <div class="card stat" style="margin-bottom: 12px">
            <span class="label">Geschätzte Kosten</span><span class="value">$ {{ r['costUsd'] }}</span>
          </div>
          <h2>Antwort</h2>
          <div class="card"><pre class="pre">{{ r['answer'] }}</pre></div>
          <h2>Tool-Provenance</h2>
          <div class="card">
            @if (provenance(r).length) {
              <table>
                <thead><tr><th>Tool</th><th>Quelle</th></tr></thead>
                <tbody>
                  @for (p of provenance(r); track $index) {
                    <tr><td class="mono">{{ p.tool }}</td><td class="small dim">{{ p.source }}</td></tr>
                  }
                </tbody>
              </table>
            } @else {
              <div class="empty">Keine Tool-Aufrufe.</div>
            }
          </div>
        } @else {
          <div class="empty">Noch kein Lauf.</div>
        }
      </div>
    </div>
  `,
})
export class TestbenchPage {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);

  systemPrompt = 'You are a concise assistant.';
  prompt = 'What time is it?';
  tools = '';

  readonly result = signal<Record<string, unknown> | null>(null);
  readonly busy = signal(false);

  provenance(r: Record<string, unknown>): { tool?: string; source?: string }[] {
    return (r['provenance'] as { tool?: string; source?: string }[] | undefined) ?? [];
  }

  run(): void {
    const tools = this.tools.split(',').map((t) => t.trim()).filter((t) => t);
    this.busy.set(true);
    this.api.testbench({ systemPrompt: this.systemPrompt, prompt: this.prompt, tools }).subscribe({
      next: (r) => {
        this.result.set(r);
        this.busy.set(false);
        this.toast.success('Testbench-Lauf abgeschlossen');
      },
      error: (err) => {
        this.busy.set(false);
        this.toast.error('Testbench-Lauf fehlgeschlagen: ' + (err?.message ?? err));
      },
    });
  }
}
