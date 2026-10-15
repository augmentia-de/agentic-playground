import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  fetchSkills,
  createAgent,
  updateAgent,
  toFlatAgentRequest,
} from './api'
import type { AgentCreateRequest } from './types'

/**
 * Characterisation / regression tests for the API client.
 *
 * WP12: the skills fetch used to exist twice (`fetchSkills` and `fetchSkillList`) and the
 * create/update agent calls duplicated a 19-field `flat` mapping. These tests pin the shared
 * behaviour so the duplication cannot creep back in.
 */

function mockFetch(payload: unknown = {}) {
  const calls: Array<{ url: string; init?: RequestInit }> = []
  const fn = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ url: String(input), init })
    return {
      ok: true,
      status: 200,
      json: async () => payload,
    } as unknown as Response
  })
  vi.stubGlobal('fetch', fn)
  return calls
}

afterEach(() => {
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

describe('api skills', () => {
  it('fetchSkills hits the canonical /skills endpoint', async () => {
    const calls = mockFetch([])
    await fetchSkills()
    expect(calls).toHaveLength(1)
    expect(calls[0].url).toContain('/skills')
  })
})

describe('api agent create/update share one mapper', () => {
  const agent: AgentCreateRequest = {
    id: 'a1',
    name: 'Agent One',
    description: 'desc',
    category: 'cat',
    model: 'gpt-x',
    temperature: 0.2,
    maxTokens: 1000,
    topP: 0.9,
    tools: ['t1'],
    guardrailsInput: ['g1'],
    guardrailsOutput: ['g2'],
    hooks: ['h1'],
    agentType: 'ua',
    systemPrompt: 'sys',
    userMessageTemplate: 'tmpl',
    jsonOutput: true,
    jsonOutputSchema: '{"type":"object"}',
    chatParameters: '{}',
    skills: ['s1'],
  }

  it('toFlatAgentRequest produces the flat wire payload', () => {
    const flat = toFlatAgentRequest(agent)
    expect(flat).toMatchObject({
      name: 'Agent One',
      model: 'gpt-x',
      tools: ['t1'],
      guardrailsInput: ['g1'],
      guardrailsOutput: ['g2'],
      hooks: ['h1'],
      agentType: 'ua',
      jsonOutput: true,
      skills: ['s1'],
    })
    // `id` is not part of the wire payload
    expect(flat).not.toHaveProperty('id')
  })

  it('create and update send the exact same payload for the same agent', async () => {
    const createCalls = mockFetch({ id: 'new-id' })
    await createAgent(agent)
    const createBody = createCalls[0].init?.body

    vi.unstubAllGlobals()
    const updateCalls = mockFetch({ id: 'a1' })
    await updateAgent('a1', agent)
    const updateBody = updateCalls[0].init?.body

    expect(updateBody).toBe(createBody)
  })
})
