import { useEffect, useState } from 'react'
import { fetchAgents, fetchMessagingChannels, fetchWorkflows } from '../../services/api'
import * as kafkaApi from '../../services/kafkaApi'
import type { AgentDefinition, MessagingChannel, WorkflowSummary } from '../../services/types'

export type ExecutionBackend = 'quarkus' | 'kafka'

/** Maps a Kafka workflow agent definition to the UI agent shape. */
function mapKafkaAgent(a: kafkaApi.KafkaAgent): AgentDefinition {
  return {
    id: a.id, name: a.name, description: '', category: a.category,
    model: a.modelKey, temperature: a.temperature, maxTokens: a.maxTokens, topP: 1,
    tools: [], guardrailsInput: [], guardrailsOutput: [], hooks: [],
    active: true, agentType: a.id, systemPrompt: '', userMessageTemplate: '',
    jsonOutput: false, jsonOutputSchema: '', chatParameters: '', timeoutSeconds: a.timeoutSeconds,
  } as unknown as AgentDefinition
}

/**
 * Loads the reference data the workflow composer needs (agents, saved workflows,
 * messaging channels, Kafka workflows) and keeps it in sync with the selected backend.
 */
export function useComposerData(backend: ExecutionBackend) {
  const [agents, setAgents] = useState<AgentDefinition[]>([])
  const [messagingChannels, setMessagingChannels] = useState<MessagingChannel[]>([])
  const [workflows, setWorkflows] = useState<WorkflowSummary[]>([])
  const [kafkaWorkflows, setKafkaWorkflows] = useState<kafkaApi.KafkaWorkflowDefinition[]>([])

  useEffect(() => {
    let cancelled = false
    fetchMessagingChannels()
      .then(data => { if (!cancelled) setMessagingChannels(data) })
      .catch(() => undefined)
    fetchWorkflows()
      .then(data => { if (!cancelled) setWorkflows(data) })
      .catch(() => undefined)
    return () => { cancelled = true }
  }, [])

  useEffect(() => {
    let cancelled = false
    if (backend === 'kafka') {
      kafkaApi.fetchKafkaAgents()
        .then(data => { if (!cancelled) setAgents(data.map(mapKafkaAgent)) })
        .catch(() => undefined)
    } else {
      fetchAgents()
        .then(data => { if (!cancelled) setAgents(data) })
        .catch(() => undefined)
    }
    return () => { cancelled = true }
  }, [backend])

  useEffect(() => {
    let cancelled = false
    if (backend === 'kafka') {
      kafkaApi.fetchKafkaWorkflows()
        .then(data => { if (!cancelled) setKafkaWorkflows(data) })
        .catch(() => undefined)
    } else {
      setKafkaWorkflows([])
    }
    return () => { cancelled = true }
  }, [backend])

  const refreshAgents = () => {
    fetchAgents()
      .then(data => setAgents(data))
      .catch(() => undefined)
  }

  return { agents, messagingChannels, workflows, kafkaWorkflows, refreshAgents }
}
