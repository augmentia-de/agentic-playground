import { Injectable, signal } from '@angular/core';

/**
 * Holds the optional client headers (tenant id + API key) in memory only.
 *
 * <p>Deliberately <b>not</b> persisted to {@code localStorage}: an API key stored there is readable
 * by any XSS payload on the page. The trade-off is that the values are lost on a reload, which is
 * acceptable for an optional developer header. If persistence is ever needed, keep the secret in a
 * short-lived in-memory session rather than web storage.
 */
@Injectable({ providedIn: 'root' })
export class ClientHeaderStore {
  private readonly tenantState = signal('');
  private readonly apiKeyState = signal('');

  readonly tenant = this.tenantState.asReadonly();
  readonly apiKey = this.apiKeyState.asReadonly();

  set(tenant: string, apiKey: string): void {
    this.tenantState.set((tenant ?? '').trim());
    this.apiKeyState.set((apiKey ?? '').trim());
  }

  clear(): void {
    this.tenantState.set('');
    this.apiKeyState.set('');
  }
}
