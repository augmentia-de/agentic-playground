#!/usr/bin/env bash
# Führt den dokumentierten Dreischritt-Workflow über Kafka aus.
#
#   ./run.sh <workflow.json> [task]
#
# Voraussetzungen: Orchestrator auf $ORCH (Default 19500), Worker mit
# WORKER_QUAD_AGENT_SOURCE=db. Siehe README.md.
set -euo pipefail

ORCH="${ORCH:-http://localhost:19500}"
HERE="$(cd "$(dirname "$0")" && pwd)"
WORKFLOW="${1:-$HERE/workflow.json}"
TASK="${2:-}"

jqp() { python3 -c "import json,sys;d=json.load(sys.stdin);print($1)"; }

echo "== 1/4 Agenten konvertieren =="
# Der Worker kennt genau einen Model-Key ("simple", aus OPENAI_*). modelId in
# agents.json muss dazu passen, sonst scheitert jeder Task mit "Unknown model key".
mapfile -t AGENTS < <(python3 -c "
import json
for a in json.load(open('$HERE/agents.json'))['agents']:
    print(json.dumps(a))
")
for a in "${AGENTS[@]}"; do
  id=$(printf '%s' "$a" | jqp "d['id']")
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$ORCH/api/v1/unified/agents/convert" \
        -H 'Content-Type: application/json' --data "$a")
  echo "  $id -> HTTP $code"
done

echo "== 2/4 Workflow konvertieren und starten =="
# Der Workflow-Name ist die Identitaet: bei gleichem Namen liefert die Bridge die
# bereits gespeicherte Definition zurueck und eine korrigierte Fassung wird ignoriert.
BODY=$(cat "$WORKFLOW")
if [ -n "$TASK" ]; then
  BODY=$(printf '%s' "$BODY" | TASK="$TASK" python3 -c "
import json,os,sys
d=json.load(sys.stdin); d['task']=os.environ['TASK']; print(json.dumps(d))")
fi

RESP=$(curl -s -X POST "$ORCH/api/v1/unified/workflows/from-quad" \
       -H 'Content-Type: application/json' -H 'X-Backend: kafka' \
       --data "$BODY")
EXEC=$(printf '%s' "$RESP" | jqp "d['executionId']")
KEY=$(printf '%s' "$RESP" | jqp "d['idempotencyKey']")
echo "  executionId=$EXEC"
echo "  idempotencyKey=$KEY  (bei Timeout mit demselben Key erneut senden)"

echo "== 3/4 Auf Ausfuehrung warten =="
for i in $(seq 1 60); do
  S=$(curl -s "$ORCH/api/v1/workflows/$EXEC")
  STATUS=$(printf '%s' "$S" | jqp "d['status']")
  printf '  [%02d] %s\n' "$i" "$STATUS"
  case "$STATUS" in
    COMPLETED|FAILED|PAUSED_HITL|TIMEOUT|CANCELLED) break ;;
  esac
  sleep 10
done

echo "== 4/4 Checkpoints =="
curl -s "$ORCH/api/v1/workflows/$EXEC" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('  status:', d['status'])
for c in d.get('checkpoints') or []:
    out=(c.get('output') or {}).get('response')
    line='  %-14s agentType=%-18s timeoutMs=%-7s %s' % (
        c['stepId'], c.get('agentType'), c.get('timeoutMs'), c['status'])
    if out: line += '  -> %s' % out.replace(chr(10),' ')[:90]
    if c.get('errorMessage'): line += '  ERROR: %s' % c['errorMessage'][:120]
    print(line)
fo=d.get('finalOutput') or {}
if fo: print('  finalOutput:', json.dumps(fo, ensure_ascii=False)[:300])
"
