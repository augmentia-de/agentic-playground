import { TestBed } from '@angular/core/testing';
import { HttpClient, provideHttpClient, withInterceptors } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { tenantInterceptor } from './tenant.interceptor';
import { ClientHeaderStore } from './client-header.store';

/**
 * WP02 / C3 — the client API key is kept in memory only and must never reach localStorage.
 */
describe('tenantInterceptor', () => {
  let http: HttpClient;
  let httpMock: HttpTestingController;
  let store: ClientHeaderStore;

  beforeEach(() => {
    localStorage.clear();
    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(withInterceptors([tenantInterceptor])),
        provideHttpClientTesting(),
      ],
    });
    http = TestBed.inject(HttpClient);
    httpMock = TestBed.inject(HttpTestingController);
    store = TestBed.inject(ClientHeaderStore);
  });

  afterEach(() => {
    httpMock.verify();
    localStorage.clear();
  });

  it('adds the API key and tenant headers from the in-memory store', () => {
    store.set('tenant-1', 'super-secret');

    http.get('/ping').subscribe();
    const req = httpMock.expectOne('/ping');

    expect(req.request.headers.get('X-API-Key')).toBe('super-secret');
    expect(req.request.headers.get('X-Tenant-Id')).toBe('tenant-1');
    req.flush({});
  });

  it('does not persist the API key to localStorage', () => {
    store.set('tenant-1', 'super-secret');

    expect(localStorage.getItem('agentflow.apiKey')).toBeNull();
    expect(localStorage.getItem('agentflow.tenant')).toBeNull();
  });

  it('sends no client headers when the store is empty', () => {
    http.get('/ping').subscribe();
    const req = httpMock.expectOne('/ping');

    expect(req.request.headers.has('X-API-Key')).toBeFalse();
    expect(req.request.headers.has('X-Tenant-Id')).toBeFalse();
    req.flush({});
  });
});
