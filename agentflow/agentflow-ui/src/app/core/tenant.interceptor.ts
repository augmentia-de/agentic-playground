import { HttpInterceptorFn } from '@angular/common/http';
import { inject } from '@angular/core';
import { ClientHeaderStore } from './client-header.store';

/**
 * Adds the optional client tenant/API-key headers so the backend's opt-in tenant scoping and
 * API-key gate can be exercised from the UI.
 *
 * <p>The values come from the in-memory {@link ClientHeaderStore}, never from {@code localStorage}
 * (see there for the rationale).
 */
export const tenantInterceptor: HttpInterceptorFn = (req, next) => {
  const store = inject(ClientHeaderStore);
  const apiKey = store.apiKey();
  const tenant = store.tenant();
  let headers = req.headers;
  if (apiKey) {
    headers = headers.set('X-API-Key', apiKey);
  }
  if (tenant) {
    headers = headers.set('X-Tenant-Id', tenant);
  }
  return next(req.clone({ headers }));
};
