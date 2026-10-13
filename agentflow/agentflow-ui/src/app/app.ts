import { Component, DestroyRef, inject, signal } from '@angular/core';
import { RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { timer } from 'rxjs';
import { ApiService } from './core/api.service';
import { ToastService } from './core/toast.service';

interface NavItem {
  path: string;
  label: string;
  icon: string;
}

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, RouterLink, RouterLinkActive],
  templateUrl: './app.html',
  styleUrl: './app.scss',
})
export class App {
  private readonly api = inject(ApiService);
  private readonly destroyRef = inject(DestroyRef);

  readonly toast = inject(ToastService);
  readonly health = signal<'up' | 'down' | 'unknown'>('unknown');
  readonly temporalUi = signal('http://localhost:8233');
  readonly theme = signal<'dark' | 'light'>('dark');

  readonly nav: NavItem[] = [
    { path: '/', label: 'Dashboard', icon: '▣' },
    { path: '/runs', label: 'Runs', icon: '▶' },
    { path: '/workflows', label: 'Workflows', icon: '⧉' },
    { path: '/generate', label: 'Generate', icon: '✦' },
    { path: '/agents', label: 'Agents', icon: '⚙' },
    { path: '/testbench', label: 'Testbench', icon: '⚗' },
    { path: '/hitl', label: 'HITL', icon: '⏸' },
    { path: '/observe', label: 'Observability', icon: '∿' },
    { path: '/examples', label: 'Examples', icon: '☰' },
    { path: '/settings', label: 'Settings', icon: '⚙' },
  ];

  constructor() {
    timer(0, 10000)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(() =>
        this.api.health().subscribe({
          next: (h) => this.health.set(h.status === 'UP' ? 'up' : 'down'),
          error: () => this.health.set('down'),
        }),
      );

    this.api.settings().subscribe({
      next: (s) => this.temporalUi.set(s.endpoints.temporalUi),
      error: () => {},
    });
  }

  toggleTheme(): void {
    const next = this.theme() === 'dark' ? 'light' : 'dark';
    this.theme.set(next);
    document.body.classList.toggle('theme-light', next === 'light');
  }
}
