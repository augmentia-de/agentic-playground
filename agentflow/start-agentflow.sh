#!/usr/bin/env bash
# ============================================================
# start-agentflow.sh — AgentFlow (Temporal + Spring Boot App [+ Angular UI])
# ------------------------------------------------------------
# Startet Temporal (Docker Compose), die App (Port 8095) und optional die
# Angular-UI (Port 4200). Danach werden Beispiel-Daten angelegt und – sofern
# gewünscht – die fünf Sample-Business-Cases gestartet.
#
# LLM (nur OpenAI-kompatibel, spring.ai.openai.*):
#   Bezugsquellen für OPENAI_API_KEY/BASE_URL/MODEL (Priorität hoch → niedrig):
#     1. CLI:  -k/--key, --base-url, --model
#     2. agentflow/.env             (app-spezifisch, optional)
#     3. quad-quarkus/.env          (SHARED OpenAI-Settings, wird geladen)
#     4. Defaults (api.openai.com / gpt-4o-mini)
#
# Header (z. B. OpenCode Session): NICHT im Skript, nur per Config
#   agentflow.llm.headers.* — z. B. via SPRING_APPLICATION_JSON. Ohne Konfiguration
#   werden keine Header gesetzt (siehe .env.example).
#
# Beispiele:
#   ./start-agentflow.sh                       # nutzt OpenAI-Settings aus quad-quarkus/.env
#   ./start-agentflow.sh --print-config        # zeigt geladene .env + effektive Werte (Key maskiert)
#   ./start-agentflow.sh -k sk-...             # Key überschreiben
#   ./start-agentflow.sh --no-ui --no-demo-runs
#   ./start-agentflow.sh --no-key --base-url http://localhost:11434/v1 --model llama3
#
# Ctrl+C beendet App/UI sauber (Temporal bleibt laufen).
# ============================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"           # .../agentflow
LOG_DIR="$(cd "${ROOT}/.." && pwd)/logs"
mkdir -p "${LOG_DIR}"

SHARED_ENV="${AGENTFLOW_SHARED_ENV:-$(cd "${ROOT}/.." && pwd)/quad-quarkus/.env}"
APP_ENV="${ROOT}/.env"

APP_PORT="${AGENTFLOW_PORT:-8095}"
APP_BASE="http://localhost:${APP_PORT}"
TEMPORAL_TARGET="${TEMPORAL_TARGET:-localhost:7233}"
TEMPORAL_UI="${AGENTFLOW_TEMPORAL_UI:-http://localhost:8233}"

ARG_KEY=""; ARG_BASE_URL=""; ARG_MODEL=""
NO_KEY=0
PRINT_CONFIG=0
START_TEMPORAL=1
START_UI=1
DO_SEED=1
DO_DEMO_RUNS=1
DO_BUILD=0

# ── Argumente ─────────────────────────────────────────────────
usage() { awk 'NR>1 && /^#/ {sub(/^# ?/,""); print; next} NR>1 {exit}' "${BASH_SOURCE[0]}"; }

while [ $# -gt 0 ]; do
  case "$1" in
    -k|--key)            ARG_KEY="$2"; shift 2 ;;
    --base-url)          ARG_BASE_URL="$2"; shift 2 ;;
    --model)             ARG_MODEL="$2"; shift 2 ;;
    --no-key)            NO_KEY=1; shift ;;
    --print-config)      PRINT_CONFIG=1; shift ;;
    --port)              APP_PORT="$2"; APP_BASE="http://localhost:${APP_PORT}"; shift 2 ;;
    --no-temporal)       START_TEMPORAL=0; shift ;;
    --no-ui)             START_UI=0; shift ;;
    --no-seed)           DO_SEED=0; shift ;;
    --no-demo-runs)      DO_DEMO_RUNS=0; shift ;;
    --build)             DO_BUILD=1; shift ;;
    -h|--help)           usage; exit 0 ;;
    *) echo "Unbekannte Option: $1" >&2; usage; exit 1 ;;
  esac
done

# ── .env-Dateien laden (shared + app) ─────────────────────────
load_env() {
  local file="$1"
  [ -f "${file}" ] || return 0
  echo "🌱 Lade ${file}"
  set +eu                                        # .env toleriert (auch bei set -u/-e)
  set -a; # shellcheck disable=SC1090
  source "${file}"; set +a
  set -eu
}

load_env "${SHARED_ENV}"      # shared OpenAI-Settings (auch QUAD_*; harmlos für agentflow)
load_env "${APP_ENV}"         # app-spezifische Overrides

# ── Warnung bei mehrfach definierten Keys (Shell: der LETZTE Wert gewinnt!) ──
warn_duplicate_keys() {
  local file="$1"
  [ -f "${file}" ] || return 0
  local key win
  while IFS= read -r key; do
    [ -n "${key}" ] || continue
    win="$(grep -E "^[[:space:]]*(export[[:space:]]+)?${key}=" "${file}" | tail -1 \
            | sed -E "s/^[[:space:]]*(export[[:space:]]+)?${key}=//" | sed -E 's/^"(.*)"$/\1/')"
    case "${key}" in *KEY*|*SECRET*|*TOKEN*) win="<masked>";; esac
    echo "⚠️  ${file}: '${key}' ist mehrfach definiert — es gilt der LETZTE Wert: ${win}" >&2
  done < <(grep -oE '^[[:space:]]*(export[[:space:]]+)?[A-Za-z_][A-Za-z0-9_]*=' "${file}" \
             | sed -E 's/^[[:space:]]*(export[[:space:]]+)?//; s/=$//' | sort | uniq -d)
}
warn_duplicate_keys "${SHARED_ENV}"
warn_duplicate_keys "${APP_ENV}"

# ── Effektive Werte (CLI > .env > Defaults) ───────────────────
BASE_URL="${ARG_BASE_URL:-${OPENAI_BASE_URL:-https://api.openai.com/v1}}"
MODEL="${ARG_MODEL:-${OPENAI_MODEL:-gpt-4o-mini}}"
API_KEY="${ARG_KEY:-${OPENAI_API_KEY:-}}"

if [ "${PRINT_CONFIG}" = "1" ]; then
  mask_key() { [ -z "${1}" ] && echo "(unset)" || { [ ${#1} -le 8 ] && echo '***' || echo "${1:0:6}..."; }; }
  echo "shared env : ${SHARED_ENV} $([ -f "${SHARED_ENV}" ] && echo '(vorhanden)' || echo '(fehlt)')"
  echo "app env    : ${APP_ENV} $([ -f "${APP_ENV}" ] && echo '(vorhanden)' || echo '(fehlt)')"
  echo "base url   : ${BASE_URL}"
  echo "model      : ${MODEL}"
  echo "api key    : $(mask_key "${API_KEY}")"
  if [ -n "${SPRING_APPLICATION_JSON:-}" ]; then
    hdr_names=$(printf '%s' "${SPRING_APPLICATION_JSON}" | grep -oE '"[A-Za-z0-9_-]+"[[:space:]]*:' | tr -d '":' | tr '\n' ' ' | sed -E 's#(agentflow|llm|headers)[[:space:]]*##g; s/  +/ /g')
    echo "llm headers: SPRING_APPLICATION_JSON gesetzt (Keys:${hdr_names:- ?})"
  else
    echo "llm headers: (keine) — SPRING_APPLICATION_JSON nicht gesetzt"
  fi
  exit 0
fi

if [ "${NO_KEY}" != "1" ] && [ -z "${API_KEY}" ]; then
  echo "❌ Kein OPENAI_API_KEY gefunden. Setze -k <KEY>, in ${APP_ENV} oder ${SHARED_ENV}, oder nutze --no-key." >&2
  exit 1
fi

export OPENAI_API_KEY="${API_KEY}"
export OPENAI_BASE_URL="${BASE_URL}"
export OPENAI_MODEL="${MODEL}"
export AGENTFLOW_PORT="${APP_PORT}"
export TEMPORAL_TARGET
export AGENTFLOW_TEMPORAL_UI="${TEMPORAL_UI}"

APP_PID=""; UI_PID=""

cleanup() {
  echo ""
  echo "🛑 Beende AgentFlow (App/UI) ..."
  [ -n "${UI_PID}" ] && kill "${UI_PID}" 2>/dev/null || true
  [ -n "${APP_PID}" ] && kill "${APP_PID}" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

command -v curl >/dev/null 2>&1 || { echo "❌ curl wird benötigt." >&2; exit 1; }
command -v mvn  >/dev/null 2>&1 || { echo "❌ mvn wird benötigt." >&2; exit 1; }
command -v java >/dev/null 2>&1 || { echo "❌ java wird benötigt." >&2; exit 1; }

# curl with a bounded time so a single stalled request can never hang the script.
http_get() { curl -sS --connect-timeout 3 --max-time "${HTTP_TIMEOUT:-10}" "$@"; }

# ── Temporal ──────────────────────────────────────────────────
start_temporal() {
  # Check the gRPC frontend (7233), NOT the UI: the UI can be up while the server is down.
  if (exec 3<>/dev/tcp/localhost/7233) >/dev/null 2>&1; then
    echo "✅ Temporal läuft bereits auf ${TEMPORAL_TARGET}"
    return
  fi
  if ! command -v docker >/dev/null 2>&1; then
    echo "⚠️  Docker nicht gefunden und kein Temporal auf :7233 — App startet trotzdem (Runs schlagen fehl)."
    return
  fi
  echo "🐳 Starte Temporal (docker compose) ..."
  (cd "${ROOT}/deploy" && docker compose up -d --remove-orphans) || {
    echo "⚠️  Temporal-Start fehlgeschlagen; fahre ohne fort."; return; }
  for _ in $(seq 1 45); do
    if (exec 3<>/dev/tcp/localhost/7233) >/dev/null 2>&1; then break; fi
    sleep 2
  done
  if ! (exec 3<>/dev/tcp/localhost/7233) >/dev/null 2>&1; then
    echo "⚠️  Temporal auf :7233 noch nicht erreichbar — siehe: docker logs agentflow-temporal" >&2
  fi
  echo "✅ Temporal bereit (gRPC :7233, UI: ${TEMPORAL_UI})"
}

# ── App ───────────────────────────────────────────────────────
app_jar() { ls "${ROOT}"/agentflow-app/target/agentflow-app-*.jar 2>/dev/null | grep -v '\.original$' | head -1; }

build_app() {
  echo "🔨 Baue AgentFlow (mvn package, skipTests) ..."
  (cd "${ROOT}" && mvn -q -DskipTests package) \
    || { echo "❌ Maven-Build fehlgeschlagen." >&2; return 1; }
}

wait_for_health() {
  echo "⏳ Warte auf ${APP_BASE}/actuator/health ..."
  for _ in $(seq 1 60); do
    if curl -sS --connect-timeout 3 --max-time 5 "${APP_BASE}/actuator/health" 2>/dev/null | grep -q '"status":"UP"'; then
      echo "✅ App ist UP"; return 0
    fi
    if ! kill -0 "${APP_PID}" 2>/dev/null; then
      echo "❌ App-Prozess beendet — siehe ${LOG_DIR}/agentflow-app.log" >&2; return 1
    fi
    sleep 2
  done
  echo "⚠️  Health-Timeout — fahre trotzdem fort (Log: ${LOG_DIR}/agentflow-app.log)" >&2
}

start_app() {
  local jar; jar="$(app_jar)"
  if [ "${DO_BUILD}" = "1" ] || [ -z "${jar}" ]; then
    build_app || return 1
    jar="$(app_jar)"
  fi
  if [ -z "${jar}" ]; then
    echo "❌ Kein ausführbares Jar unter agentflow-app/target/ gefunden." >&2; return 1
  fi
  echo "🚀 Starte AgentFlow App auf Port ${APP_PORT} (model=${MODEL}, base-url=${BASE_URL}) ..."
  echo "   Jar: ${jar}"
  java -jar "${jar}" > "${LOG_DIR}/agentflow-app.log" 2>&1 &
  APP_PID=$!
  echo "   Log: ${LOG_DIR}/agentflow-app.log (PID ${APP_PID})"
  wait_for_health || true
}

# ── UI ────────────────────────────────────────────────────────
start_ui() {
  local ui_dir="${ROOT}/agentflow-ui"
  [ -d "${ui_dir}" ] || return 0
  if ! command -v npm >/dev/null 2>&1; then
    echo "⚠️  npm nicht gefunden — überspringe UI."; return 0
  fi
  if [ ! -d "${ui_dir}/node_modules" ]; then
    echo "🎨 Installiere UI-Dependencies (npm install) ..."
    (cd "${ui_dir}" && npm install)
  fi
  echo "🎨 Starte Angular-UI auf Port 4200 ..."
  (cd "${ui_dir}" && npm start -- --port 4200) > "${LOG_DIR}/agentflow-ui.log" 2>&1 &
  UI_PID=$!
  echo "   UI: http://localhost:4200  (Log: ${LOG_DIR}/agentflow-ui.log)"
}

# ── Beispieldaten ─────────────────────────────────────────────
post() { curl -sS --connect-timeout 3 --max-time 15 -X POST "$@" || true; }

seed_data() {
  echo ""
  echo "🌱 Lege Beispiel-Daten an ..."

  echo "   • Info/Define/Capabilities (Selbsttest):"
  http_get "${APP_BASE}/agentflow/info" >/dev/null && echo "     info ok"
  local wf cap
  wf="$(http_get "${APP_BASE}/agentflow/workflows"   2>/dev/null || echo '[]')"
  cap="$(http_get "${APP_BASE}/agentflow/capabilities" 2>/dev/null || echo '[]')"
  echo "     Workflow-Definitionen: ${wf}"
  echo "     Capabilities: $(echo "${cap}" | head -c 240)..."

  echo "   • Memory (remember/recall) für Scope 'project-agentflow':"
  post "${APP_BASE}/agentflow/runs/memory?op=remember&scope=project-agentflow&content=AgentFlow%20nutzt%20Temporal%20%2B%20LangGraph4j%20%2B%20Spring%20AI" >/dev/null && echo "     remember ok"
  post "${APP_BASE}/agentflow/runs/memory?op=remember&scope=project-agentflow&content=LLM%20kommt%20aus%20spring.ai.openai.*" >/dev/null && echo "     remember ok"
  post "${APP_BASE}/agentflow/runs/memory?op=recall&scope=project-agentflow" >/dev/null && echo "     recall ok"

  echo "   • Capability-Suche (RAG):"
  http_get "${APP_BASE}/agentflow/capabilities/search?q=web&topK=3" >/dev/null && echo "     search ok"

  if [ "${DO_DEMO_RUNS}" = "1" ]; then
    echo ""
    echo "🎬 Starte Beispiel-Business-Cases (dauert je nach LLM einige Sekunden) ..."
    start_run "Support-Triage"        "${APP_BASE}/examples/support-triage?customerId=c-demo&message=Meine%20Rechnung%20ist%20falsch"
    start_run "Contract-Review"       "${APP_BASE}/examples/contract-review?contract=Der%20Lieferant%20kann%20jederzeit%20kuendigen."
    start_run "Invoice (paid, 50)"    "${APP_BASE}/examples/invoice?invoiceId=inv-demo-1&amount=50"
    start_run "Invoice (compensated, 500)" "${APP_BASE}/examples/invoice?invoiceId=inv-demo-2&amount=500"
    start_run "Market-Research"       "${APP_BASE}/examples/market-research?topic=Cargo%20bikes%20in%20European%20cities"
    start_run "Onboarding"            "${APP_BASE}/examples/onboarding?newHire=Ada&projectScope=project-agentflow"
    echo ""
    echo "   • Deklarativer Workflow (v2-Dokument):"
    start_run "Declarative market-research" "${APP_BASE}/agentflow/runs/declarative?workflowId=market-research&task=EV%20batteries%20in%20Europe"
    echo ""
    echo "   👉 Status/Logs: ${APP_BASE}/agentflow/runs  ·  Temporal-UI: ${TEMPORAL_UI}"
  else
    echo "   (—) Demo-Runs übersprungen (--no-demo-runs)"
  fi
}

start_run() {
  local label="$1" url="$2" resp id
  resp="$(curl -sS --connect-timeout 3 --max-time 30 -X POST "${url}" 2>/dev/null || echo '{}')"
  id="$(echo "${resp}" | sed -n 's/.*"workflowId"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
  printf "     • %-26s → %s\n" "${label}" "${id:-$resp}"
}

# ── Banner ────────────────────────────────────────────────────
KEY_STATE="gesetzt"; [ -z "${API_KEY}" ] && KEY_STATE="(kein Key)"
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║                    AgentFlow                              ║"
echo "╠══════════════════════════════════════════════════════════╣"
printf "║  App:        %-42s ║\n" "${APP_BASE}"
printf "║  UI:         %-42s ║\n" "http://localhost:4200"
printf "║  Temporal:   %-42s ║\n" "${TEMPORAL_TARGET}  (UI ${TEMPORAL_UI})"
printf "║  Model:      %-42s ║\n" "${MODEL}"
printf "║  Base URL:   %-42s ║\n" "${BASE_URL}"
printf "║  API-Key:    %-42s ║\n" "${KEY_STATE}"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

[ "${START_TEMPORAL}" = "1" ] && start_temporal
start_app
[ "${START_UI}" = "1" ] && start_ui
[ "${DO_SEED}" = "1" ] && seed_data

echo ""
echo "✅ AgentFlow läuft. Ctrl+C beendet App/UI (Temporal bleibt aktiv)."
echo "   App-Log: ${LOG_DIR}/agentflow-app.log"
[ -n "${UI_PID}" ] && echo "   UI-Log:  ${LOG_DIR}/agentflow-ui.log"

if [ -n "${APP_PID}" ]; then wait "${APP_PID}"; fi
