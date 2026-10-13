#!/usr/bin/env bash
# ============================================================
# llm-smoke.sh — Minimaler LLM-Aufruf mit DENSELBEN Settings wie AgentFlow
# ------------------------------------------------------------
# Lädt (wie start-agentflow.sh):
#   1. ../quad-quarkus/.env   (OPENAI_API_KEY/BASE_URL/MODEL)
#   2. ./agentflow/.env       (Overrides, z. B. agentflow.llm.headers via SPRING_APPLICATION_JSON)
# und sendet EINEN Chat-Completion-Request inkl. der konfigurierten Header
# (User-Agent, x-opencode-session, ...). Platzhalter {{requestId}}/{{sessionId}} werden
# mit dem Fallback (agentflow.llm.fallback-request-id) ersetzt, {{apiKey}} mit dem Key.
#
# Nutzung:
#   ./llm-smoke.sh                       # Standard-Prompt
#   ./llm-smoke.sh "Sag kurz Hallo."     # eigener Prompt
#
# So prüfst du Key/Base-URL/Modell/Header isoliert (ohne Temporal/App).
# Für den vollen agentflow-Pfad: POST /agentflow/testbench/run (siehe README).
# ============================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SHARED_ENV="${AGENTFLOW_SHARED_ENV:-$(cd "${ROOT}/.." && pwd)/quad-quarkus/.env}"
APP_ENV="${ROOT}/.env"
PROMPT="${1:-Sag in einem kurzen Satz Hallo.}"

command -v curl >/dev/null 2>&1 || { echo "❌ curl wird benötigt." >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "❌ python3 wird benötigt (JSON-Parsing)." >&2; exit 1; }

load_env() { [ -f "$1" ] || return 0; set +eu; set -a; . "$1"; set +a; set -eu; }
load_env "${SHARED_ENV}"
load_env "${APP_ENV}"

BASE_URL="${OPENAI_BASE_URL:-https://api.openai.com/v1}"
MODEL="${OPENAI_MODEL:-gpt-4o-mini}"
KEY="${OPENAI_API_KEY:-}"
REQ_ID="${AGENTFLOW_LLM_FALLBACK_REQUEST_ID:-agentflow-smoke}"

mask() { [ -z "$1" ] && echo "(unset)" || { [ ${#1} -le 8 ] && echo '***' || echo "${1:0:6}..."; }; }

# Header aus SPRING_APPLICATION_JSON.agentflow.llm.headers auflösen
mapfile -t HEADER_PAIRS < <(
  if [ -n "${SPRING_APPLICATION_JSON:-}" ]; then
    printf '%s' "${SPRING_APPLICATION_JSON}" | python3 -c '
import sys, json
try:
    h = json.load(sys.stdin)["agentflow"]["llm"]["headers"]
except Exception:
    sys.exit(0)
for k, v in h.items():
    print(k + "\t" + str(v))
'
  fi
)

HEADER_ARGS=()
echo "base url : ${BASE_URL}"
echo "model    : ${MODEL}"
echo "api key  : $(mask "${KEY}")"
for pair in "${HEADER_PAIRS[@]:-}"; do
  [ -n "${pair}" ] || continue
  name="${pair%%$'\t'*}"
  value="${pair#*$'\t'}"
  value="${value//\{\{requestId\}\}/${REQ_ID}}"
  value="${value//\{\{sessionId\}\}/${REQ_ID}}"
  value="${value//\{\{apiKey\}\}/${KEY}}"
  HEADER_ARGS+=(-H "${name}: ${value}")
  echo "header   : ${name}: ${value}"
done
[ "${#HEADER_ARGS[@]}" -eq 0 ] && echo "header   : (keine konfiguriert)"
echo "prompt   : ${PROMPT}"
echo

BODY="$(MODEL="${MODEL}" PROMPT="${PROMPT}" python3 -c '
import json, os
print(json.dumps({
    "model": os.environ["MODEL"],
    "messages": [{"role": "user", "content": os.environ["PROMPT"]}],
    "max_tokens": 64,
}))')"

echo "── Antwort ────────────────────────────────────────────────"
curl -sS --connect-timeout 5 --max-time 90 "${BASE_URL}/chat/completions" \
  -H "Authorization: Bearer ${KEY}" \
  -H "Content-Type: application/json" \
  "${HEADER_ARGS[@]}" \
  -d "${BODY}" \
  -w $'\nHTTP %{http_code}\n'
