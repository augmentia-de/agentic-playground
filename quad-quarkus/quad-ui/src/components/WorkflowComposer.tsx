import { useEffect, useRef, useState } from 'react'
import { useWorkflow } from '../contexts/WorkflowContext'
import { createWorkflow, executeWorkflow, fetchRun, fetchWorkflow, updateWorkflow, materializeWorkflowAgents, dematerializeWorkflowAgents } from '../services/api'
import * as kafkaApi from '../services/kafkaApi'
import { WorkflowList } from './WorkflowList'
import type { MessagingChannel, WorkflowRun } from '../services/types'
import { ReactFlowCanvas } from './ReactFlowCanvas'
import { ComposerToolbar } from './workflow/ComposerToolbar'
import { WorkflowSettings } from './workflow/WorkflowSettings'
import { NodeInspector } from './workflow/NodeInspector'
import { EdgeEditor } from './workflow/EdgeEditor'
import { RunResultPanel } from './workflow/RunResultPanel'
import { ComposerDialogs } from './workflow/ComposerDialogs'
import { getAvailableTargets } from './workflow/graphUtils'
import { useComposerData, type ExecutionBackend } from './workflow/useComposerData'
import './WorkflowComposer.css'

interface WorkflowComposerProps {
  onTabChange?: (tab: string) => void
}

export function WorkflowComposer({ onTabChange }: WorkflowComposerProps) {
  const {
    state,
    dispatch,
    addNode,
    updateNode,
    removeNode,
    updateEdge,
    removeEdge,
    setNodeStatuses,
    setName,
    setId,
    newWorkflow,
    loadWorkflow,
    executeWorkflow: markRunning,
    resetWorkflow,
  } = useWorkflow()

  const isSaved = state.id !== 'default'
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)
  const [initialTask, setInitialTask] = useState('')
  const [insertAgentId, setInsertAgentId] = useState<string>('')
  /** Shared prefix (Block A) — workflow level. Persisted via the workflow save payload. */
  const [cachePrefix, setCachePrefix] = useState('')
  const [prefixCaching, setPrefixCaching] = useState(false)
  const [running, setRunning] = useState(false)
  const [saving, setSaving] = useState(false)
  const [materializing, setMaterializing] = useState(false)
  const [saveMessage, setSaveMessage] = useState<string | null>(null)
  const [lastRun, setLastRun] = useState<WorkflowRun | null>(null)
  const [runError, setRunError] = useState<string | null>(null)
  const [schemaEditorOpen, setSchemaEditorOpen] = useState(false)
  const [editEdgeInputId, setEditEdgeInputId] = useState<string | null>(null)
  const [backend, setBackend] = useState<ExecutionBackend>('quarkus')
  const startTaskRef = useRef<HTMLTextAreaElement>(null)

  const { agents, messagingChannels, workflows, kafkaWorkflows, refreshAgents } = useComposerData(backend)

  const hasMessagingIn = state.nodes.some(n => n.type === 'messaging-in')

  // Auto-grow the start-task textarea with its content; collapses back to one line when empty
  const autogrowStartTask = (el: HTMLTextAreaElement) => {
    el.style.height = 'auto'
    el.style.height = `${el.scrollHeight}px`
  }

  useEffect(() => {
    if (startTaskRef.current) autogrowStartTask(startTaskRef.current)
  }, [initialTask])

  const selectedNode = state.nodes.find(n => n.id === selectedNodeId) ?? null

  // Downstream helpers for selector options
  const downstreamTargets = (nodeId: string) => getAvailableTargets(nodeId, state.nodes, state.edges)

  /** Creates a messaging-in node pre-filled from a channel configuration */
  const addMessagingInFromChannel = (channel: MessagingChannel) => {
    const id = `node-${Date.now()}-${Math.random().toString(36).slice(2, 11)}`
    const newNode = {
      id,
      type: 'messaging-in' as const,
      title: `In: ${channel.topic}`,
      status: 'pending' as const,
      createdAt: new Date().toISOString(),
      config: {
        topic: channel.topic,
        tenantId: channel.tenantId || 'default',
        agentId: channel.agentId,
        timeoutMs: 30000,
      },
    }
    // Direct dispatch - messaging-in is a start node, no auto-edge
    dispatch({ type: 'ADD_NODE', payload: newNode })
  }

  /** Creates the workflow or updates it — returns the workflow id */
  const saveWorkflow = async (): Promise<string> => {
    const payload = { name: state.name, nodes: state.nodes, edges: state.edges, initialTask, cachePrefix, prefixCaching }
    if (isSaved) {
      await updateWorkflow(state.id, payload)
      return state.id
    }
    const id = await createWorkflow(payload)
    setId(id)
    return id
  }

  const handleSave = async () => {
    if (state.nodes.length === 0) {
      setSaveMessage('Nothing to save -- add nodes first')
      return
    }
    setSaving(true)
    setSaveMessage(null)
    try {
      const id = await saveWorkflow()
      setSaveMessage(id === state.id ? `Workflow updated: ${id}` : `Workflow saved: ${id}`)
    } catch (err) {
      setSaveMessage('Save failed: ' + String(err))
    } finally {
      setSaving(false)
    }
  }

  /** Runs a workflow run and polls until completion */
  const runWorkflowById = async (workflowId: string) => {
    setRunError(null)
    setLastRun(null)
    markRunning()
    const { runId } = await executeWorkflow(workflowId, initialTask)
    let finished = false
    while (!finished) {
      await new Promise(resolve => setTimeout(resolve, 1000))
      try {
        const run = await fetchRun(runId)
        setLastRun(run)
        const statuses: Record<string, 'pending' | 'running' | 'waiting' | 'completed' | 'failed'> = {}
        for (const nr of run.nodeResults) {
          statuses[nr.nodeId] = nr.status
        }
        setNodeStatuses(statuses)
        if (run.status === 'completed' || run.status === 'failed') {
          finished = true
          if (run.status === 'failed') setRunError(`Run ${run.runId} failed`)
        }
      } catch {
        finished = true
        setRunError('Could not load run status')
      }
    }
  }

  /** Kafka-based execution with polling on checkpoints endpoint */
  const runKafkaWorkflow = async (executionId: string) => {
    setRunError(null)
    setLastRun(null)
    markRunning()
    let finished = false
    while (!finished) {
      await new Promise(resolve => setTimeout(resolve, 2000))
      try {
        const status = await kafkaApi.getKafkaWorkflowStatus(executionId)
        const checkpoints = status.checkpoints ?? []
        const statuses: Record<string, 'pending' | 'running' | 'waiting' | 'completed' | 'failed'> = {}
        for (const cp of checkpoints) {
          statuses[cp.stepId] = cp.status.toLowerCase().includes('complete') ? 'completed'
            : cp.status.toLowerCase().includes('fail') ? 'failed'
            : cp.status.toLowerCase().includes('wait') ? 'waiting'
            : 'running' as const
        }
        setNodeStatuses(statuses)
        const runStatus: 'running' | 'completed' | 'failed' =
          status.status === 'COMPLETED' ? 'completed'
            : status.status === 'FAILED' ? 'failed'
            : 'running'
        setLastRun({
          runId: executionId,
          workflowId: status.workflowName,
          status: runStatus,
          startedAt: status.startedAt,
          finishedAt: status.finishedAt ?? null,
          durationMs: status.finishedAt ? Date.parse(status.finishedAt) - Date.parse(status.startedAt) : 0,
          nodeResults: checkpoints.map(cp => ({
            nodeId: cp.stepId,
            type: cp.agentType || 'step',
            title: cp.stepId,
            status: statuses[cp.stepId] || 'pending',
            output: '',
            input: '',
          })),
          initialData: '',
        })
        if (status.status === 'COMPLETED' || status.status === 'FAILED' || status.status === 'CANCELLED') {
          finished = true
          if (status.status === 'FAILED') setRunError(`Workflow ${status.workflowName} failed`)
        }
      } catch {
        finished = true
        setRunError('Could not load Kafka workflow status')
      }
    }
  }

  const handleExecute = async () => {
    if (state.nodes.length === 0 || running) return
    setRunning(true)
    try {
      if (backend === 'kafka') {
        // Convert + persist (shared DB) + start in one atomic call via the gateway.
        const resp = await kafkaApi.convertQuadToKafka(
          state.name,
          state.nodes.map(n => ({
            id: n.id,
            type: n.type,
            agentType: (n.config?.agentId as string) || undefined,
          })),
          state.edges.map(e => ({ source: e.source, target: e.target, condition: '' })),
          initialTask
        )
        setSaveMessage(`Kafka: ${resp.nodeCount} nodes → execution ${resp.executionId}`)
        await runKafkaWorkflow(resp.executionId)
      } else {
        const id = await saveWorkflow()
        await runWorkflowById(id)
      }
    } catch (error) {
      state.nodes.forEach(node => updateNode(node.id, { status: 'failed' }))
      setRunError(String(error))
      setRunning(false)
    }
  }

  const handleMaterialize = async () => {
    if (materializing) return
    setMaterializing(true)
    setSaveMessage(null)
    try {
      const res = await materializeWorkflowAgents(state.id)
      setSaveMessage(`Agents aktiviert: ${res.activated} neu, ${res.referenced} referenziert — sichtbar im Agents-Tab.`)
    } catch (err) {
      setSaveMessage(`Fehler beim Aktivieren: ${String(err)}`)
    } finally {
      setMaterializing(false)
    }
  }

  const handleDematerialize = async () => {
    if (materializing) return
    setMaterializing(true)
    setSaveMessage(null)
    try {
      const res = await dematerializeWorkflowAgents(state.id)
      setSaveMessage(`Agents verborgen: ${res.deactivated} deaktiviert, ${res.referenced} referenziert.`)
    } catch (err) {
      setSaveMessage(`Fehler beim Verbergen: ${String(err)}`)
    } finally {
      setMaterializing(false)
    }
  }

  const handleLoadWorkflow = (id: string) => {
    fetchWorkflow(id)
      .then(wf => {
        loadWorkflow(wf.id, wf.name, wf.nodes, wf.edges)
        setInitialTask(wf.initialTask ?? '')
        setCachePrefix(wf.cachePrefix ?? '')
        setPrefixCaching(Boolean(wf.prefixCaching))
        setSelectedNodeId(null)
        setLastRun(null)
        setSaveMessage(null)
      })
      .catch(() => undefined)
  }

  /** Loads a saved workflow and runs it directly */
  const handleRunWorkflow = async (id: string) => {
    if (running) return
    setRunning(true)
    try {
      const wf = await fetchWorkflow(id)
      loadWorkflow(wf.id, wf.name, wf.nodes, wf.edges)
      setInitialTask(wf.initialTask ?? '')
      setCachePrefix(wf.cachePrefix ?? '')
      setPrefixCaching(Boolean(wf.prefixCaching))
      setSelectedNodeId(null)
      await runWorkflowById(id)
    } catch (err) {
      setRunError(String(err))
    } finally {
      setRunning(false)
    }
  }

  const handleNewWorkflow = () => {
    newWorkflow()
    setInitialTask('')
    setCachePrefix('')
    setPrefixCaching(false)
    setSelectedNodeId(null)
    setLastRun(null)
    setSaveMessage(null)
    setRunError(null)
  }

  return (
    <div className="workflow-composer" data-testid="workflow-composer">
      <h2>Workflow Composer</h2>

      <div className="workflow-name-row">
        <label>Name:</label>
        <input
          type="text"
          value={state.name}
          onChange={e => setName(e.target.value)}
          aria-label="Workflow-Name"
        />
        <span className="workflow-save-state">
          {isSaved ? `Saved: ${state.id}` : 'Not saved'}
        </span>
      </div>

      <ComposerToolbar
        agents={agents}
        kafkaWorkflows={kafkaWorkflows}
        backend={backend}
        nodeCount={state.nodes.length}
        isSaved={isSaved}
        saving={saving}
        running={running}
        materializing={materializing}
        insertAgentId={insertAgentId}
        selectedNodeId={selectedNodeId}
        onBackendChange={setBackend}
        onInsertAgentIdChange={setInsertAgentId}
        onAddNode={addNode}
        onNewWorkflow={handleNewWorkflow}
        onReset={resetWorkflow}
        onSave={handleSave}
        onExecute={handleExecute}
        onMaterialize={handleMaterialize}
        onDematerialize={handleDematerialize}
      />

      {saveMessage && <p className="save-message">{saveMessage}</p>}

      <WorkflowSettings
        prefixCaching={prefixCaching}
        cachePrefix={cachePrefix}
        onPrefixCachingChange={setPrefixCaching}
        onCachePrefixChange={setCachePrefix}
        hasMessagingIn={hasMessagingIn}
        initialTask={initialTask}
        startTaskRef={startTaskRef}
        onInitialTaskChange={(value, el) => {
          setInitialTask(value)
          autogrowStartTask(el)
        }}
        messagingChannels={messagingChannels}
        onAddMessagingIn={addMessagingInFromChannel}
      />

      {running && <div className="workflow-progress" role="progressbar"><p>Workflow running -- loading status...</p></div>}
      {runError && <p className="error-state">{runError}</p>}

      <div className="workflow-editor" role="region" aria-label="Workflow Editor">
        {state.nodes.length === 0 ? (
          <div className="empty-state">
            <p>Add agents using the toolbar to start building your workflow</p>
          </div>
        ) : (
          <ReactFlowCanvas
            nodes={state.nodes}
            edges={state.edges}
            agents={agents}
            selectedNodeId={selectedNodeId}
            onSelectNode={setSelectedNodeId}
            onAddEdge={(edge) => dispatch({ type: 'ADD_EDGE', payload: edge })}
            onRemoveEdge={(id) => dispatch({ type: 'REMOVE_EDGE', payload: id })}
            onUpdateNode={updateNode}
          />
        )}
      </div>

      <div className="workflow-config-grid">
        {selectedNode && (
          <NodeInspector
            node={selectedNode}
            agents={agents}
            messagingChannels={messagingChannels}
            workflows={workflows}
            downstreamTargets={downstreamTargets}
            onUpdateNode={updateNode}
            onRemoveNode={(id) => { removeNode(id); setSelectedNodeId(null) }}
            onRefreshAgents={refreshAgents}
            onOpenSchema={() => setSchemaEditorOpen(true)}
          />
        )}

        <EdgeEditor
          nodes={state.nodes}
          edges={state.edges}
          onUpdateEdge={updateEdge}
          onRemoveEdge={removeEdge}
          onEditInputs={setEditEdgeInputId}
        />
      </div>

      <RunResultPanel lastRun={lastRun} onTabChange={onTabChange} />

      <WorkflowList onSelect={handleLoadWorkflow} onExecute={handleRunWorkflow} />

      <ComposerDialogs
        schemaEditorOpen={schemaEditorOpen}
        schemaNode={selectedNode}
        onSchemaSave={schema => {
          if (!selectedNode) return
          updateNode(selectedNode.id, {
            config: { ...selectedNode.config, jsonOutputSchema: schema || undefined },
          })
        }}
        onSchemaClose={() => setSchemaEditorOpen(false)}
        editEdgeInputId={editEdgeInputId}
        nodes={state.nodes}
        edges={state.edges}
        onEdgeInputsSave={(edgeId, inputs) => {
          updateEdge(edgeId, {
            inputs: inputs.length > 0 ? inputs : undefined,
            input: inputs.length === 1 ? inputs[0] : undefined,
          })
          setEditEdgeInputId(null)
        }}
        onEdgeInputsClose={() => setEditEdgeInputId(null)}
      />
    </div>
  )
}
