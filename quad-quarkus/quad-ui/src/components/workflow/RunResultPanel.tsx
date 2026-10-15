import type { WorkflowRun } from '../../services/types'

export interface RunResultPanelProps {
  lastRun: WorkflowRun | null
  onTabChange?: (tab: string) => void
}

export function RunResultPanel({ lastRun, onTabChange }: RunResultPanelProps) {
  if (!lastRun) return null
  return (
    <section className="run-result panel">
      <h3>Last Run: {lastRun.runId}</h3>
      <p>
        Status: <strong>{lastRun.status}</strong> · Duration: {lastRun.durationMs} ms
        {' · '}{lastRun.startedAt}
      </p>
      {lastRun.initialData && (
        <div className="run-start-task">
          <span className="run-start-task-label">Start task:</span>{' '}
          <span className="run-start-task-value">{lastRun.initialData}</span>
        </div>
      )}
      {lastRun.status === 'completed' && lastRun.nodeResults.length > 0 && (
        <div className="run-summary">
          <h4>Workflow Result</h4>
          <pre className="run-output">{lastRun.nodeResults[lastRun.nodeResults.length - 1].output}</pre>
        </div>
      )}
      {onTabChange && (
        <button type="button" onClick={() => onTabChange('monitor')}>View in Monitor</button>
      )}
      {lastRun.nodeResults.map(nr => (
        <details key={nr.nodeId} open={nr.status === 'failed'}>
          <summary className={`node-summary ${nr.status}`}>
            [{nr.status.toUpperCase()}] {nr.title}
          </summary>
          {nr.input && (
            <>
              <h4>Injected Input (SessionState):</h4>
              <pre className="run-input">{nr.input}</pre>
            </>
          )}
          <h4>Output:</h4>
          <pre className="run-output">{nr.output}</pre>
        </details>
      ))}
    </section>
  )
}
