import { Component, OnInit, inject, input, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ApiService } from '../core/api.service';
import { ENGINES } from '../core/engines';
import { StartEngine, StartEngineField } from '../core/models';
import { ToastService } from '../core/toast.service';

interface SelectOption {
  value: string | number;
  label: string;
}

/**
 * Data-driven start form: renders the fields of the selected engine and POSTs to its path.
 */
@Component({
  selector: 'af-start-form',
  imports: [FormsModule],
  template: `
    <div class="card">
      <h3>Run starten</h3>

      <div class="field">
        <label>Engine</label>
        <select [(ngModel)]="selectedKey" (ngModelChange)="onEngineChange()" [ngModelOptions]="{ standalone: true }">
          @for (e of engines; track e.key) {
            <option [ngValue]="e.key">{{ e.label }}</option>
          }
        </select>
      </div>

      @if (engine; as e) {
        @for (f of e.fields; track f.name) {
          <div class="field">
            <label>{{ f.label }}{{ f.required ? ' *' : '' }}</label>
            @if (f.type === 'textarea') {
              <textarea
                [(ngModel)]="values[f.name]"
                [ngModelOptions]="{ standalone: true }"
                [placeholder]="f.placeholder || ''"
              ></textarea>
            } @else if (f.type === 'select') {
              <select [(ngModel)]="values[f.name]" [ngModelOptions]="{ standalone: true }">
                @if (!optionsFor(f).length) {
                  <option [ngValue]="''" disabled>{{ f.optionsFrom ? '(lädt …)' : '(keine Optionen)' }}</option>
                }
                @for (o of optionsFor(f); track o.value) {
                  <option [ngValue]="o.value">{{ o.label }}</option>
                }
              </select>
            } @else {
              <input
                [type]="f.type"
                [(ngModel)]="values[f.name]"
                [ngModelOptions]="{ standalone: true }"
                [placeholder]="f.placeholder || ''"
              />
            }
          </div>
        }
        @if (e.hint) {
          <p class="small dim">{{ e.hint }}</p>
        }
        <div class="row">
          <button class="primary" (click)="submit()" [disabled]="busy()">Starten</button>
          @if (result()) {
            <span class="small mono">{{ result() }}</span>
          }
        </div>
      }
    </div>
  `,
})
export class StartForm implements OnInit {
  private readonly api = inject(ApiService);
  private readonly toast = inject(ToastService);

  readonly engines = ENGINES;
  engineKey = input<string | undefined>(undefined);

  selectedKey = ENGINES[0].key;
  values: Record<string, string | number> = {};
  readonly busy = signal(false);
  readonly result = signal('');
  /** Options resolved from REST, keyed by field name (overrides static {@code options}). */
  private readonly dynamic = signal<Record<string, SelectOption[]>>({});

  ngOnInit(): void {
    if (this.engineKey()) {
      this.selectedKey = this.engineKey()!;
    }
    this.onEngineChange();
  }

  get engine(): StartEngine | undefined {
    return this.engines.find((e) => e.key === this.selectedKey);
  }

  /** Options for a select field: REST-resolved if present, else the static list. */
  optionsFor(f: StartEngineField): SelectOption[] {
    const dyn = this.dynamic()[f.name];
    if (dyn) {
      return dyn;
    }
    return (f.options ?? []).map((o) => ({ value: o, label: o }));
  }

  onEngineChange(): void {
    this.result.set('');
    this.values = {};
    this.dynamic.set({});
    for (const field of this.engine?.fields ?? []) {
      this.values[field.name] = field.defaultValue ?? '';
    }
    this.loadDynamicOptions();
  }

  /** Loads options for fields declared with {@code optionsFrom} (e.g. declarative workflows). */
  private loadDynamicOptions(): void {
    for (const field of this.engine?.fields ?? []) {
      if (field.optionsFrom !== 'declarative-workflows') {
        continue;
      }
      this.api.workflows().subscribe({
        next: (list) => {
          const opts: SelectOption[] = list.map((w) => ({
            value: w.id,
            label: w.name ? `${w.name} (${w.id})` : w.id,
          }));
          this.dynamic.update((m) => ({ ...m, [field.name]: opts }));
          if (!String(this.values[field.name] ?? '').trim() && opts.length) {
            this.values[field.name] = opts[0].value;
          }
        },
        error: (err) => {
          this.dynamic.update((m) => ({ ...m, [field.name]: [] }));
          this.toast.error('Workflows laden fehlgeschlagen: ' + (err?.message ?? err?.status ?? err));
        },
      });
    }
  }

  submit(): void {
    const e = this.engine;
    if (!e) {
      return;
    }
    const missing = e.fields
      .filter((f) => f.required && !String(this.values[f.name] ?? '').trim())
      .map((f) => f.label);
    if (missing.length) {
      this.toast.error('Pflichtfelder fehlen: ' + missing.join(', '));
      return;
    }
    this.busy.set(true);
    this.result.set('');
    this.api.start(e.path, this.values).subscribe({
      next: (r) => {
        const id = String(r['workflowId'] ?? '');
        this.result.set(id ? `workflowId: ${id}` : JSON.stringify(r));
        this.toast.success(`Gestartet (${e.label})${id ? ': ' + id : ''}`);
        this.busy.set(false);
      },
      error: (err) => {
        this.toast.error('Start fehlgeschlagen: ' + (err?.message ?? err?.status ?? err));
        this.busy.set(false);
      },
    });
  }
}
