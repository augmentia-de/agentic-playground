import { Injectable } from '@angular/core';
import { RunDetail, RunEvent, RunSummary } from './models';

/** Server-Sent-Events client for live run updates (with graceful degradation to polling). */
@Injectable({ providedIn: 'root' })
export class RunStreamService {
  /** Streams the run list; returns a disposer. */
  streamList(
    query: string,
    limit: number,
    onData: (runs: RunSummary[]) => void,
    onError: () => void,
  ): () => void {
    const url = `/agentflow/runs/stream?query=${encodeURIComponent(query)}&limit=${limit}`;
    const source = new EventSource(url);
    source.addEventListener('runs', (event) => {
      try {
        const parsed = JSON.parse((event as MessageEvent).data);
        // Guard against a double-encoded frame (JSON string instead of array).
        if (Array.isArray(parsed)) {
          onData(parsed as RunSummary[]);
        }
      } catch {
        /* ignore malformed frame */
      }
    });
    source.onerror = () => onError();
    return () => source.close();
  }

  /** Streams a single run: status/result frames and new history-event frames. */
  streamRun(
    workflowId: string,
    onRun: (run: RunDetail) => void,
    onEvents: (events: RunEvent[]) => void,
    onError: () => void,
  ): () => void {
    const url = `/agentflow/runs/${encodeURIComponent(workflowId)}/stream`;
    const source = new EventSource(url);
    source.addEventListener('run', (event) => {
      try {
        const run = JSON.parse((event as MessageEvent).data) as RunDetail;
        if (run && typeof run === 'object' && !Array.isArray(run)) {
          onRun(run);
          if (run.status && run.status !== 'RUNNING') {
            source.close();
          }
        }
      } catch {
        /* ignore */
      }
    });
    source.addEventListener('events', (event) => {
      try {
        const parsed = JSON.parse((event as MessageEvent).data) as RunEvent[];
        if (Array.isArray(parsed)) {
          onEvents(parsed);
        }
      } catch {
        /* ignore */
      }
    });
    source.onerror = () => onError();
    return () => source.close();
  }
}
