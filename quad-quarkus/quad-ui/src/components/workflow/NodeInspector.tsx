import type { AgentDefinition, MessagingChannel, WorkflowNode, WorkflowSummary } from '../../services/types'
import { PrefixEditor } from './PrefixEditor'

export interface NodeInspectorProps {
  node: WorkflowNode
  agents: AgentDefinition[]
  messagingChannels: MessagingChannel[]
  workflows: WorkflowSummary[]
  downstreamTargets: (nodeId: string) => Array<{ id: string; title: string }>
  onUpdateNode: (id: string, updates: Partial<WorkflowNode>) => void
  onRemoveNode: (id: string) => void
  onRefreshAgents: () => void
  onOpenSchema: () => void
}

export function NodeInspector({ node, agents, messagingChannels, workflows, downstreamTargets, onUpdateNode, onRemoveNode, onRefreshAgents, onOpenSchema }: NodeInspectorProps) {
  return (
          <section className="node-editor panel">
            <h3>Node: {node.title}</h3>
            <div className="form-group">
              <label>Title</label>
              <input
                type="text"
                value={node.title}
                onChange={e => onUpdateNode(node.id, { title: e.target.value })}
              />
            </div>

            <div className="form-group">
              <label>Timeout (ms, 0 = kein Limit)</label>
              <input
                type="number"
                min="0"
                value={(node.config?.timeoutMs as number) ?? 0}
                onChange={e => onUpdateNode(node.id, {
                  config: { ...node.config, timeoutMs: parseInt(e.target.value) || 0 },
                })}
              />
              <p className="field-hint">
                <code>0</code> = deaktiviert (unbegrenzt warten). Bei Überschreitung wird ein
                Timeout-Marker in den Node-Output geschrieben — ein nachfolgender Conditional kann
                auf <code>contains:timeout</code> routen.
              </p>
            </div>

            {node.type === 'agent' && (
              <>
                <div className="form-group">
                  <label>Bound Agent</label>
                  <div className="agent-select-row">
                    <select
                      value={(node.config?.agentId as string) ?? ''}
                      onChange={e => onUpdateNode(node.id, {
                        config: { ...node.config, agentId: e.target.value || undefined },
                      })}
                    >
                      <option value="">-- no agent bound --</option>
                      {agents.map(a => (
                        <option key={a.id} value={a.id}>{a.name} ({a.category})</option>
                      ))}
                    </select>
                    <button type="button" className="small" onClick={onRefreshAgents} title="Refresh agent list">
                      ↻
                    </button>
                  </div>
                </div>
                <div className="prefix-section">
                  <h4>Prefix (Block A)</h4>
                  <PrefixEditor
                    config={node.config}
                    onChange={cfg => onUpdateNode(node.id, { config: cfg })}
                  />
                </div>
                <div className="form-group">
                  <label>
                    <input
                      type="checkbox"
                      checked={(node.config?.jsonOutput as boolean) ?? false}
                      onChange={e => onUpdateNode(node.id, {
                        config: { ...node.config, jsonOutput: e.target.checked },
                      })}
                    />
                    {' '}JSON Output (produces structured JSON response)
                  </label>
                </div>
                {(node.config?.jsonOutput as boolean) && (
                  <div className="form-group">
                    <button
                      type="button"
                      className="small"
                      onClick={() => onOpenSchema()}
                      aria-label="JSON Output Schema bearbeiten"
                    >
                      ✎ JSON-Output-Schema{(node.config?.jsonOutputSchema as string) ? ' (active)' : ' (off)'}
                    </button>
                    <p className="field-hint">
                      Valid JSON Schema wird als Structured Output (responseFormat) durchgesetzt.
                    </p>
                  </div>
                )}
                <div className="form-group">
                  <label>System Prompt</label>
                  <textarea
                    value={(node.config?.systemPrompt as string) ?? ''}
                    onChange={e => onUpdateNode(node.id, {
                      config: { ...node.config, systemPrompt: e.target.value || undefined },
                    })}
                    placeholder="Role and instructions for the LLM (optional)"
                    rows={3}
                  />
                </div>
                {(node.config?.jsonOutput as boolean) && (
                  <div className="form-group">
                    <label>User Message Template</label>
                    <textarea
                      value={(node.config?.userMessageTemplate as string) ?? ''}
                      onChange={e => onUpdateNode(node.id, {
                        config: { ...node.config, userMessageTemplate: e.target.value || undefined },
                      })}
                      placeholder="Template with {{nodeId.output}}, {{initialPrompt}}, ..."
                      rows={4}
                    />
                  </div>
                )}
              </>
            )}

            {node.type === 'loop' && (
              <>
                <div className="form-group">
                  <label>Max Iterations</label>
                  <input
                    type="number"
                    min="1"
                    value={(node.config?.maxIterations as number) ?? 5}
                    onChange={e => onUpdateNode(node.id, {
                      config: { ...node.config, maxIterations: parseInt(e.target.value) || 1 },
                    })}
                  />
                </div>
                <div className="form-group">
                  <label>Exit Condition</label>
                  <input
                    type="text"
                    value={(node.config?.exitCondition as string) ?? ''}
                    onChange={e => onUpdateNode(node.id, {
                      config: { ...node.config, exitCondition: e.target.value },
                    })}
                    placeholder="e.g. contains:fertig | iter>=3 | matches:score=\\d+"
                  />
                  <p className="field-hint">
                    Leave empty to always run maxIterations. Supported: always, contains:text,
                    notcontains:text, equals:text, matches:regex, iter==N, iter!=N, iter&lt;N, iter&gt;=N…
                  </p>
                </div>

                {/* NEW: Loop Anchor selector — default = entire downstream segment */}
                <div className="form-group">
                  <label>Loop Start (first body node)</label>
                  <select
                    value={(node.config?.loopTargetId as string) ?? ''}
                    onChange={e => onUpdateNode(node.id, {
                      config: { ...node.config, loopTargetId: e.target.value || undefined },
                    })}
                    aria-label="Loop start (first body node)"
                  >
                    <option value="">-- all downstream agents --</option>
                    {downstreamTargets(node.id).map(t => (
                      <option key={t.id} value={t.id}>{t.title}</option>
                    ))}
                  </select>
                  <p className="field-hint">
                    Default (-- all downstream agents --) repeats EVERY agent below this loop node in the
                    same iteration, all the way until the flow links back to the loop node. Optionally pick
                    a start anchor to repeat only the segment from that node onward.
                  </p>
                </div>
              </>
            )}

            {node.type === 'conditional' && (
              <>
                <div className="form-group">
                  <label>Condition</label>
                  <input
                    type="text"
                    value={(node.config?.condition as string) ?? ''}
                    onChange={e => onUpdateNode(node.id, {
                      config: { ...node.config, condition: e.target.value },
                    })}
                    placeholder="e.g. contains:error"
                  />
                </div>
                <p className="field-hint">
                  Evaluated against the combined input of this node. Use the selectors below to route
                  to specific agent segments instead of skipping everything.
                </p>

                {/* NEW: True Target selector */}
                <div className="form-group">
                  <label>Then →</label>
                  <select
                    value={(node.config?.trueTargetId as string) ?? ''}
                    onChange={e => onUpdateNode(node.id, {
                      config: { ...node.config, trueTargetId: e.target.value || undefined },
                    })}
                    aria-label="Then target"
                  >
                    <option value="">-- next step in sequence --</option>
                    {downstreamTargets(node.id).map(t => (
                      <option key={t.id} value={t.id}>{t.title}</option>
                    ))}
                  </select>
                </div>

                {/* NEW: False Target selector */}
                <div className="form-group">
                  <label>Else →</label>
                  <select
                    value={(node.config?.falseTargetId as string) ?? ''}
                    onChange={e => onUpdateNode(node.id, {
                      config: { ...node.config, falseTargetId: e.target.value || undefined },
                    })}
                    aria-label="Else target"
                  >
                    <option value="">-- skip downstream --</option>
                    {downstreamTargets(node.id).map(t => (
                      <option key={t.id} value={t.id}>{t.title}</option>
                    ))}
                  </select>
                </div>
              </>
            )}

            {node.type === 'nested-workflow' && (
              <>
                <div className="form-group">
                  <label>Target Workflow</label>
                  <select
                    value={(node.config?.workflowId as string) ?? ''}
                    onChange={e => onUpdateNode(node.id, {
                      config: { ...node.config, workflowId: e.target.value },
                    })}
                  >
                    <option value="">-- choose workflow --</option>
                    {workflows.map(w => (
                      <option key={w.id} value={w.id}>{w.name} ({w.id})</option>
                    ))}
                  </select>
                </div>
                <div className="form-group">
                  <label>Output Mapping (subWorkflowNodeId → parentKey)</label>
                  {Object.entries((node.config?.outputMapping as Record<string, string>) ?? {}).map(([subKey, parentKey]) => (
                    <div className="mapping-row" key={subKey}>
                      <input
                        type="text"
                        value={subKey}
                        aria-label="Sub workflow node id"
                        onChange={e => {
                          const next: Record<string, string> = { ...(node.config?.outputMapping as Record<string, string> ?? {}) }
                          const value = next[subKey]
                          delete next[subKey]
                          next[e.target.value] = value
                          onUpdateNode(node.id, { config: { ...node.config, outputMapping: next } })
                        }}
                      />
                      <span className="mapping-arrow">→</span>
                      <input
                        type="text"
                        value={parentKey ?? ''}
                        aria-label="Parent scope key"
                        onChange={e => onUpdateNode(node.id, {
                          config: {
                            ...node.config,
                            outputMapping: { ...(node.config?.outputMapping as Record<string, string> ?? {}), [subKey]: e.target.value },
                          },
                        })}
                      />
                      <button
                        type="button"
                        className="danger small"
                        onClick={() => {
                          const next: Record<string, string> = { ...(node.config?.outputMapping as Record<string, string> ?? {}) }
                          delete next[subKey]
                          onUpdateNode(node.id, { config: { ...node.config, outputMapping: next } })
                        }}
                      >
                        ✕
                      </button>
                    </div>
                  ))}
                  <button
                    type="button"
                    className="small"
                    onClick={() => onUpdateNode(node.id, {
                      config: {
                        ...node.config,
                        outputMapping: {
                          ...(node.config?.outputMapping as Record<string, string> ?? {}),
                          [`node-${Date.now()}`]: '',
                        },
                      },
                    })}
                  >
                    + Add mapping
                  </button>
                </div>
              </>
            )}

            {node.type === 'async' && (
              <>
                <p className="help-text">
                  Runs the following node (or <code>refNodeId</code>) asynchronously and
                  waits for the result up to the value set in the "Timeout" field (deferred completion).
                  When the timeout expires (<code>0</code> = no limit), a timeout marker is written
                  into the output — a following conditional can then route on, e.g.,
                  <code> contains:timeout</code>.
                </p>
                <p className="help-text">
                  <strong>External async:</strong> submit the order via <code>messaging-out</code>,
                  the external system returns the result via{' '}
                  <code>POST /api/ui/dynamic/complete {"{runId, nodeId, output}"}</code>.
                  Async nodes run in parallel (virtual threads); a <code>join</code> aggregates
                  multiple async branches.
                </p>
              </>
            )}

            {node.type === 'fork' && (
              <p className="help-text">
                Parallel branch marker: the incoming and outgoing paths run in parallel via the scheduler.
              </p>
            )}

            {node.type === 'join' && (
              <>
                <div className="form-group">
                  <label>Join Policy</label>
                  <select
                    value={(node.config?.joinPolicy as string) ?? 'all'}
                    onChange={e => onUpdateNode(node.id, {
                      config: { ...node.config, joinPolicy: e.target.value },
                    })}
                  >
                    <option value="all">all</option>
                    <option value="any">any</option>
                  </select>
                </div>
                <div className="form-group">
                  <label>Result IDs (Komma-getrennt)</label>
                  <input
                    type="text"
                    value={((node.config?.resultIds as string[]) ?? []).join(',')}
                    onChange={e => onUpdateNode(node.id, {
                      config: {
                        ...node.config,
                        resultIds: e.target.value.split(',').map(s => s.trim()).filter(Boolean),
                      },
                    })}
                    placeholder="node-A,node-B,node-C"
                  />
                </div>
                <p className="help-text">
                  <code>all</code> = wait for all incoming branches (including async branches),
                  <code> any</code> = continue as soon as one is done. With the timeout field above,
                  the join fires as soon as all expected result IDs are present <b>or</b> the timeout
                  expires (<code>0</code> = no limit); missing results are marked with
                  <code> timeout</code>. A following aggregator agent gets the
                  results as input, or a conditional routes on <code>contains:timeout</code>.
                </p>
              </>
            )}

            {node.type === 'messaging-in' && (
              <>
                {messagingChannels.length > 0 && !(node.config?.topic) && (
                  <div className="form-group">
                    <label>Select Channel</label>
                    <select
                      onChange={e => {
                        const ch = messagingChannels[parseInt(e.target.value)]
                        if (ch) {
                          onUpdateNode(node.id, {
                            config: {
                              ...node.config,
                              topic: ch.topic,
                              tenantId: ch.tenantId || 'default',
                              agentId: ch.agentId,
                              timeoutMs: (node.config?.timeoutMs as number) || 30000,
                            },
                            title: `In: ${ch.topic}`,
                          })
                        }
                      }}
                    >
                      <option value="">-- choose channel --</option>
                      {messagingChannels.map((ch, i) => (
                        <option key={i} value={i}>{ch.transport}: {ch.topic} (agent: {ch.agentId})</option>
                      ))}
                    </select>
                  </div>
                )}
                <div className="form-group">
                  <label>Topic</label>
                  <input
                    type="text"
                    value={(node.config?.topic as string) ?? ''}
                    onChange={e => onUpdateNode(node.id, {
                      config: { ...node.config, topic: e.target.value },
                    })}
                    placeholder="e.g. support-commands"
                  />
                </div>
                <div className="form-group">
                  <label>Tenant ID</label>
                  <input
                    type="text"
                    value={(node.config?.tenantId as string) ?? 'default'}
                    onChange={e => onUpdateNode(node.id, {
                      config: { ...node.config, tenantId: e.target.value || 'default' },
                    })}
                  />
                </div>
              </>
            )}

            {node.type === 'messaging-out' && (
              <>
                <div className="form-group">
                  <label>Channel</label>
                  <select
                    value={(node.config?.channel as string) ?? 'kafka'}
                    onChange={e => onUpdateNode(node.id, {
                      config: { ...node.config, channel: e.target.value },
                    })}
                  >
                    <option value="kafka">Kafka</option>
                    <option value="amqp">AMQP (RabbitMQ)</option>
                    <option value="email">Email</option>
                  </select>
                </div>
                <div className="form-group">
                  <label>Topic / Queue</label>
                  <input
                    type="text"
                    value={(node.config?.topic as string) ?? ''}
                    onChange={e => onUpdateNode(node.id, {
                      config: { ...node.config, topic: e.target.value },
                    })}
                    placeholder="e.g. quad-results"
                  />
                </div>
              </>
            )}

            <button
              type="button"
              className="danger"
              onClick={() => { onRemoveNode(node.id) }}
            >
              Remove Node
            </button>
          </section>
  )
}
