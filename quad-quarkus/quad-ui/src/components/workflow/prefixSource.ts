/**
 * Prefix-source helpers for the workflow composer (Block A / prefix caching).
 *
 * Extracted from `WorkflowComposer.tsx` (WP12). The persisted value shape mirrors the backend
 * `PrefixSourceResolver` so the UI stays byte-compatible with what the runtime expects.
 */

export type PrefixSourceType = 'inline' | 'url' | 'file' | 'step' | 'self'

export const PREFIX_SOURCE_TYPES: Array<{ value: PrefixSourceType; label: string }> = [
  { value: 'inline', label: 'Inline text' },
  { value: 'url', label: 'URL (fetched at runtime)' },
  { value: 'file', label: 'File (session workspace)' },
  { value: 'step', label: 'Step output reference' },
  { value: 'self', label: 'Self (this node)' },
]

/** Detects the prefixSource kind from the persisted config value (auto-detect rules of PrefixSourceResolver). */
export function prefixSourceTypeOf(raw: unknown): PrefixSourceType {
  if (raw && typeof raw === 'object' && !Array.isArray(raw)) {
    const m = raw as Record<string, unknown>
    if (typeof m.url === 'string') return 'url'
    if (typeof m.file === 'string') return 'file'
    if (typeof m.step === 'string') return 'step'
    if (m.self === true) return 'self'
  }
  return 'inline'
}

/** Extracts the editable value for the current prefixSource type. */
export function prefixSourceValueOf(raw: unknown): string {
  if (raw == null) return ''
  if (typeof raw === 'string') return raw
  const m = raw as Record<string, unknown>
  for (const key of ['url', 'file', 'step'] as const) {
    if (typeof m[key] === 'string') return m[key]
  }
  return JSON.stringify(raw)
}

/** Builds the persisted prefixSource value from type + value (mirror of PrefixSourceResolver). */
export function prefixSourceToConfig(type: PrefixSourceType, value: string): unknown {
  if (type === 'inline') return value
  if (type === 'self') return { self: true }
  return { [type]: value }
}
