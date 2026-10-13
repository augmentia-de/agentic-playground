import { HttpInterceptorFn } from '@angular/common/http';

/**
 * Adds the optional client tenant/API-key headers from localStorage so the backend's opt-in
 * tenant scoping and API-key gate can be exercised from the UI.
 */
export const tenantInterceptor: HttpInterceptorFn = (req, next) => {
  const apiKey = localStorage.getItem('agentflow.apiKey');
  const tenant = localStorage.getItem('agentflow.tenant');
  let headers = req.headers;
  if (apiKey) {
    headers = headers.set('X-API-Key', apiKey);
  }
  if (tenant) {
    headers = headers.set('X-Tenant-Id', tenant);
  }
  return next(req.clone({ headers }));
};
