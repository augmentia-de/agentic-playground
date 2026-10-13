import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';
import {
  Capability,
  ExampleCase,
  Health,
  RunDetail,
  RunEvent,
  RunSummary,
  Settings,
  ValidateResult,
  WorkflowDefinitionSummary,
  WorkflowDoc,
} from './models';

/** Thin, typed wrapper around the agentflow-app REST API (same origin / dev proxy). */
@Injectable({ providedIn: 'root' })
export class ApiService {
  private readonly http = inject(HttpClient);

  /// ── Observability / info ───────────────────────────────────────────────
  health(): Observable<Health> {
    return this.http.get<Health>('/actuator/health');
  }

  info(): Observable<Record<string, unknown>> {
    return this.http.get<Record<string, unknown>>('/agentflow/info');
  }

  prometheus(): Observable<string> {
    return this.http.get('/actuator/prometheus', { responseType: 'text' });
  }

  settings(): Observable<Settings> {
    return this.http.get<Settings>('/agentflow/settings');
  }

  /// ── Runs (Temporal visibility) ─────────────────────────────────────────
  runs(query = '', limit = 50): Observable<RunSummary[]> {
    return this.http.get<RunSummary[]>('/agentflow/runs', {
      params: new HttpParams().set('query', query).set('limit', String(limit)),
    });
  }

  run(workflowId: string): Observable<RunDetail> {
    return this.http.get<RunDetail>('/agentflow/runs/' + encodeURIComponent(workflowId));
  }

  runEvents(workflowId: string, max = 500): Observable<RunEvent[]> {
    return this.http.get<RunEvent[]>('/agentflow/runs/' + encodeURIComponent(workflowId) + '/events', {
      params: new HttpParams().set('max', String(max)),
    });
  }

  terminate(workflowId: string, reason = 'terminated via UI'): Observable<unknown> {
    return this.http.post('/agentflow/runs/' + encodeURIComponent(workflowId) + '/terminate', null, {
      params: new HttpParams().set('reason', reason),
    });
  }

  /// ── Capabilities / roles ───────────────────────────────────────────────
  capabilities(): Observable<Capability[]> {
    return this.http.get<Capability[]>('/agentflow/capabilities');
  }

  searchCapabilities(q: string, topK = 10): Observable<Capability[]> {
    return this.http.get<Capability[]>('/agentflow/capabilities/search', {
      params: new HttpParams().set('q', q).set('topK', String(topK)),
    });
  }

  quadCapabilities(): Observable<Record<string, unknown>> {
    return this.http.get<Record<string, unknown>>('/agentflow/quad/capabilities');
  }

  roles(): Observable<Record<string, unknown>> {
    return this.http.get<Record<string, unknown>>('/agentflow/security/roles');
  }

  /// ── Declarative workflows + authoring ──────────────────────────────────
  workflows(): Observable<WorkflowDefinitionSummary[]> {
    return this.http.get<WorkflowDefinitionSummary[]>('/agentflow/workflows');
  }

  workflow(id: string): Observable<WorkflowDoc> {
    return this.http.get<WorkflowDoc>('/agentflow/workflows/' + encodeURIComponent(id));
  }

  validateWorkflow(doc: WorkflowDoc): Observable<ValidateResult> {
    return this.http.post<ValidateResult>('/agentflow/workflows/validate', doc);
  }

  saveWorkflow(id: string, doc: WorkflowDoc): Observable<WorkflowDefinitionSummary> {
    return this.http.put<WorkflowDefinitionSummary>('/agentflow/workflows/' + encodeURIComponent(id), doc);
  }

  /** Creates a new, empty declarative workflow (single end node) for authoring. */
  createWorkflow(name?: string): Observable<WorkflowDefinitionSummary> {
    return this.http.post<WorkflowDefinitionSummary>('/agentflow/workflows', { name });
  }

  /// ── Dynamic workflow generation (LLM) ──────────────────────────────────
  /** Decomposes a task into a v2 workflow document (preview, not persisted). */
  generateWorkflow(task: string, name?: string): Observable<{ workflow: WorkflowDoc }> {
    return this.http.post<{ workflow: WorkflowDoc }>('/agentflow/workflows/generate', { task, name });
  }

  /** Generates a workflow and persists it as a new definition. */
  saveGeneratedWorkflow(task: string, name?: string): Observable<WorkflowDefinitionSummary> {
    return this.http.post<WorkflowDefinitionSummary>('/agentflow/workflows/generate/save', { task, name });
  }

  /** Generates a workflow and runs it durably on Temporal. */
  runGeneratedWorkflow(task: string, name?: string): Observable<Record<string, unknown>> {
    return this.http.post<Record<string, unknown>>('/agentflow/workflows/generate/run', { task, name });
  }

  /// ── Examples ───────────────────────────────────────────────────────────
  examples(): Observable<ExampleCase[]> {
    return this.http.get<ExampleCase[]>('/examples');
  }

  /// ── Generic starters ───────────────────────────────────────────────────

  /** Starts a workflow via POST with query parameters (mirrors the backend endpoints). */
  start(path: string, params: Record<string, string | number | boolean | undefined>): Observable<Record<string, unknown>> {
    let httpParams = new HttpParams();
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== null && value !== '') {
        httpParams = httpParams.set(key, String(value));
      }
    }
    return this.http.post<Record<string, unknown>>(path, null, { params: httpParams });
  }

  /// ── Signals (HITL / declarative / escalate) ────────────────────────────
  hitlDecide(workflowId: string, approved: boolean): Observable<unknown> {
    return this.start(`/agentflow/runs/hitl/${encodeURIComponent(workflowId)}/decide`, { approved });
  }

  declarativeApprove(workflowId: string, approved: boolean): Observable<unknown> {
    return this.start(`/agentflow/runs/declarative/${encodeURIComponent(workflowId)}/approve`, { approved });
  }

  escalate(workflowId: string, approved: boolean): Observable<unknown> {
    return this.start(`/examples/support-triage/${encodeURIComponent(workflowId)}/escalate`, { approved });
  }

  /// ── Authoring history ──────────────────────────────────────────────────
  workflowVersions(id: string): Observable<string[]> {
    return this.http.get<string[]>(`/agentflow/workflows/${encodeURIComponent(id)}/versions`);
  }

  workflowVersion(id: string, version: string): Observable<WorkflowDoc> {
    return this.http.get<WorkflowDoc>(
      `/agentflow/workflows/${encodeURIComponent(id)}/versions/${encodeURIComponent(version)}`,
    );
  }

  /// ── Agent testbench ────────────────────────────────────────────────────
  testbench(request: { systemPrompt?: string; prompt: string; tools: string[] }): Observable<Record<string, unknown>> {
    return this.http.post<Record<string, unknown>>('/agentflow/testbench/run', request);
  }
}
