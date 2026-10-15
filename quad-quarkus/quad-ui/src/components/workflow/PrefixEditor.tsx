import { prefixSourceToConfig, prefixSourceTypeOf, prefixSourceValueOf, PREFIX_SOURCE_TYPES } from './prefixSource'
import type { PrefixSourceType } from './prefixSource'

interface PrefixEditorProps {
  config?: Record<string, unknown>
  onChange: (updates: Record<string, unknown>) => void
}

export function PrefixEditor({ config, onChange }: PrefixEditorProps) {
  const prefixType = prefixSourceTypeOf(config?.prefixSource)
  const prefixValue = prefixSourceValueOf(config?.prefixSource)
  const usePrefix = config?.usePrefix
  return (
    <>
      <div className="form-group">
        <label>Use prefix</label>
        <select
          value={usePrefix === true ? 'true' : usePrefix === false ? 'false' : ''}
          onChange={e => {
            const cfg = { ...config }
            const v = e.target.value
            if (v === 'true') cfg.usePrefix = true
            else if (v === 'false') cfg.usePrefix = false
            else delete cfg.usePrefix
            onChange(cfg)
          }}
        >
          <option value="">Workflow default</option>
          <option value="true">On</option>
          <option value="false">Off</option>
        </select>
      </div>

      <div className="form-group">
        <label>
          <input
            type="checkbox"
            checked={config?.clearPrefix === true}
            onChange={e => {
              const cfg = { ...config }
              if (e.target.checked) cfg.clearPrefix = true
              else delete cfg.clearPrefix
              onChange(cfg)
            }}
          />
          {' '}Clear inherited prefix (start fresh)
        </label>
      </div>

      <div className="form-group">
        <label>Node cache prefix (local override)</label>
        <textarea
          rows={2}
          className="cache-prefix-input"
          value={(config?.cachePrefix as string) ?? ''}
          onChange={e => {
            const cfg = { ...config }
            if (e.target.value) cfg.cachePrefix = e.target.value
            else delete cfg.cachePrefix
            onChange(cfg)
          }}
          placeholder="Replaces the workflow-level prefix for this node."
        />
      </div>

      <div className="form-group">
        <label>Prefix source</label>
        <select
          value={prefixType}
          onChange={e => {
            const t = e.target.value as PrefixSourceType
            onChange({ ...config, prefixSource: prefixSourceToConfig(t, prefixValue) })
          }}
        >
          {PREFIX_SOURCE_TYPES.map(opt => (
            <option key={opt.value} value={opt.value}>{opt.label}</option>
          ))}
        </select>
        {prefixType !== 'self' && (
          <textarea
            rows={2}
            className="cache-prefix-input"
            value={prefixValue}
            onChange={e => {
              onChange({ ...config, prefixSource: prefixSourceToConfig(prefixType, e.target.value) })
            }}
            placeholder={
              prefixType === 'url'
                ? 'https://example.com/prefix.md'
                : prefixType === 'file'
                  ? 'path/relative/workspace or file://abs/path'
                  : prefixType === 'step'
                    ? 'target node id (e.g. node-... )'
                    : 'Static prefix text loaded at runtime (overrides cache prefix).'
            }
          />
        )}
        <p className="field-hint">
          Resolved at runtime: inline text, a URL, a workspace file or a prior step's output. Must stay
          byte-identical across runs to hit the provider cache.
        </p>
      </div>
    </>
  )
}
