/** Small formatting/UI helpers shared by the pages. */

export function statusClass(status?: string | null): string {
  switch ((status ?? '').toUpperCase()) {
    case 'RUNNING':
      return 'badge running';
    case 'COMPLETED':
      return 'badge ok';
    case 'FAILED':
    case 'TIMED_OUT':
      return 'badge error';
    case 'CANCELED':
    case 'TERMINATED':
      return 'badge muted';
    case 'CONTINUED_AS_NEW':
      return 'badge running';
    default:
      return 'badge';
  }
}

/** True only while a run can still be terminated (i.e. it is actively running). */
export function isRunning(status?: string | null): boolean {
  return (status ?? '').toUpperCase() === 'RUNNING';
}

export function pretty(value: unknown): string {
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'string') {
    try {
      return JSON.stringify(JSON.parse(value), null, 2);
    } catch {
      return value;
    }
  }
  return JSON.stringify(value, null, 2);
}

export function timeAgo(iso?: string | null): string {
  if (!iso) {
    return '–';
  }
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) {
    return iso;
  }
  const seconds = Math.round((Date.now() - then) / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h`;
  return `${Math.round(hours / 24)}d`;
}

/** Extracts the {@code agent_*} families from the raw Prometheus exposition text. */
export function parseAgentMetrics(text: string): { name: string; value: string }[] {
  const out = new Map<string, string>();
  for (const line of text.split('\n')) {
    if (!line || line.startsWith('#')) continue;
    const match = /^(agent_[a-zA-Z0-9_]+)(?:\{[^}]*\})?\s+([-\d.eE+]+)/.exec(line);
    if (match) {
      const name = match[1];
      const value = Number(match[2]);
      const current = Number(out.get(name) ?? '0');
      out.set(name, String(Number.isNaN(current) ? match[2] : current + value));
    }
  }
  return [...out.entries()].map(([name, value]) => ({ name, value })).sort((a, b) => a.name.localeCompare(b.name));
}
