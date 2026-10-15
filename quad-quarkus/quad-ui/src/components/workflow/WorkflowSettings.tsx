import type { RefObject } from 'react'
import type { MessagingChannel } from '../../services/types'

export interface WorkflowSettingsProps {
  /** Workflow-level shared prefix (Block A). */
  prefixCaching: boolean
  cachePrefix: string
  onPrefixCachingChange: (value: boolean) => void
  onCachePrefixChange: (value: string) => void
  /** Start / initial task. */
  hasMessagingIn: boolean
  initialTask: string
  startTaskRef: RefObject<HTMLTextAreaElement | null>
  onInitialTaskChange: (value: string, el: HTMLTextAreaElement) => void
  /** Available messaging channels for quick messaging-in insertion. */
  messagingChannels: MessagingChannel[]
  onAddMessagingIn: (channel: MessagingChannel) => void
}

/**
 * Workflow-level settings: shared prefix caching (Block A), the start/initial task and the messaging
 * channel shortcuts. Extracted from `WorkflowComposer.tsx` (WP12).
 */
export function WorkflowSettings({
  prefixCaching,
  cachePrefix,
  onPrefixCachingChange,
  onCachePrefixChange,
  hasMessagingIn,
  initialTask,
  startTaskRef,
  onInitialTaskChange,
  messagingChannels,
  onAddMessagingIn,
}: WorkflowSettingsProps) {
  return (
    <>
      <div className="shared-prefix-panel">
        <label className="shared-prefix-toggle">
          <input
            type="checkbox"
            checked={prefixCaching}
            onChange={e => onPrefixCachingChange(e.target.checked)}
          />
          {' '}Shared Prefix Caching (Block A) — stable prefix for LLM cache hits
        </label>
        {prefixCaching && (
          <div className="form-group">
            <label>Cache Prefix (Block A)</label>
            <textarea
              className="cache-prefix-input"
              value={cachePrefix}
              onChange={e => onCachePrefixChange(e.target.value)}
              placeholder="Static system instructions / tool guide shared by every agent node — must stay byte-identical across runs."
              rows={3}
            />
            <p className="field-hint">
              Applied to every agent node without an explicit per-node override. Agent nodes must register
              byte-identical native tools, otherwise the run fails fast on the shared-tool guard.
            </p>
          </div>
        )}
      </div>

      <div className="start-task-row">
        <label htmlFor="start-task-input">Start / Initial Task:</label>
        {hasMessagingIn ? (
          <p className="start-task-hint">
            Workflow starts from a messaging-in node — the start task is ignored on execution.
          </p>
        ) : (
          <textarea
            id="start-task-input"
            ref={startTaskRef}
            rows={1}
            className="start-task-input"
            value={initialTask}
            onChange={e => onInitialTaskChange(e.target.value, e.target)}
            placeholder="e.g. Analyze customer complaints and propose a solution"
            aria-label="Start / Initial Task"
          />
        )}
      </div>

      {messagingChannels.length > 0 && (
        <div className="messaging-channels-bar">
          <span className="messaging-channels-label">Messaging Channels:</span>
          {messagingChannels.map((ch, i) => (
            <button
              key={i}
              type="button"
              className="messaging channel-btn"
              onClick={() => onAddMessagingIn(ch)}
              title={`Add messaging-in node for ${ch.topic} (${ch.transport})`}
            >
              + {ch.transport}:{ch.topic}
            </button>
          ))}
        </div>
      )}
    </>
  )
}
