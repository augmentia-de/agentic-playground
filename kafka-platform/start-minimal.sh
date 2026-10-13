#!/bin/bash
# ============================================================
# start-minimal.sh — minimaler Kafka-Stack: Orchestrator + QUAD-Worker
# ------------------------------------------------------------
# Startet NUR die zwei Anwendungscontainer. Postgres und Kafka
# werden NICHT mitdeployt — sie sind als externe Infrastruktur
# vorausgesetzt. Kein Redis, kein Vault, kein Jaeger, kein
# Prometheus, kein Grafana, kein Legacy-Worker.
#
#   ./start-minimal.sh            # build + start + health-wait
#   ./start-minimal.sh stop       # beide Container stoppen
#   ./start-minimal.sh status     # Status + Health
#   ./start-minimal.sh logs       # Logs beider Services folgen
#
# Pflicht-Umgebungsvariablen (kein Default — Compose bricht sonst ab):
#   EXTERNAL_KAFKA_BOOTSTRAP_SERVERS   z.B. kafka-1:9092,kafka-2:9092
#   POSTGRES_URL                      jdbc:postgresql://host:5432/agent_platform
#   POSTGRES_USER
#   POSTGRES_PASSWORD
#   OPENAI_API_KEY
#   OPENAI_MODEL
#
# Optional:
#   OPENAI_BASE_URL            Default https://openrouter.ai/api/v1
#   SKIP_BUILD=1               kein mvn, nur docker compose up
#
# Beispiele:
#   EXTERNAL_KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
#   POSTGRES_URL=jdbc:postgresql://pg:5432/agent_platform \
#   POSTGRES_USER=agent POSTGRES_PASSWORD=secret \
#   OPENAI_API_KEY=sk-... OPENAI_MODEL=deepseek/deepseek-v4-flash-0731 \
#   ./start-minimal.sh
# ============================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUAD_DIR="$(cd "${ROOT}/../quad-quarkus" && pwd)"
COMPOSE_FILE="${ROOT}/docker-compose.minimal.yml"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-240}"
ORCH_URL="http://localhost:8080/actuator/health"
WORKER_URL="http://localhost:8092/actuator/health"

RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; BLUE=$'\033[34m'; OFF=$'\033[0m'
die()  { echo "${RED}❌ $*${OFF}" >&2; exit 1; }
info() { echo "${BLUE}ℹ${OFF}  $*"; }
ok()   { echo "${GREEN}✅${OFF} $*"; }
warn() { echo "${YELLOW}⚠️ ${OFF} $*"; }

compose() { docker compose -f "${COMPOSE_FILE}" "$@"; }

# Fuer stop/status/logs braucht compose nur die Variablen zur Interpolation,
# nicht ihre Werte. Ohne diese Platzhalter bricht schon `docker compose down`
# ab, weil die Datei ${VAR:?...} verwendet. Deshalb nur fuer diese Befehle.
compose_placeholders() {
    for v in EXTERNAL_KAFKA_BOOTSTRAP_SERVERS POSTGRES_URL POSTGRES_USER \
             POSTGRES_PASSWORD OPENAI_API_KEY OPENAI_MODEL; do
        [ -n "${!v:-}" ] || export "${v}=placeholder"
    done
    export OPENAI_BASE_URL="${OPENAI_BASE_URL:-https://openrouter.ai/api/v1}"
}

# ── Umgebungsvariablen ────────────────────────────────────────
check_env() {
    local missing=()
    for v in EXTERNAL_KAFKA_BOOTSTRAP_SERVERS POSTGRES_URL POSTGRES_USER \
             POSTGRES_PASSWORD OPENAI_API_KEY OPENAI_MODEL; do
        [ -n "${!v:-}" ] || missing+=("${v}")
    done
    if [ "${#missing[@]}" -gt 0 ]; then
        echo "${RED}❌ Fehlende Umgebungsvariablen:${OFF}" >&2
        printf '     %s\n' "${missing[@]}" >&2
        echo "     → Siehe Kopf dieses Skripts für ein vollständiges Beispiel." >&2
        exit 1
    fi
    export OPENAI_BASE_URL="${OPENAI_BASE_URL:-https://openrouter.ai/api/v1}"
}

# ── Werkzeuge ─────────────────────────────────────────────────
check_tools() {
    command -v docker >/dev/null || die "docker nicht gefunden"
    docker compose version >/dev/null 2>&1 || die "'docker compose' (V2) nicht verfügbar"
    docker info >/dev/null 2>&1 || die "Docker-Daemon nicht erreichbar"
    if [ "${SKIP_BUILD:-0}" != "1" ]; then
        command -v mvn >/dev/null || die "mvn nicht gefunden (oder SKIP_BUILD=1 setzen)"
    fi
    [ -f "${COMPOSE_FILE}" ] || die "Compose-Datei fehlt: ${COMPOSE_FILE}"
    ok "Voraussetzungen erfüllt"
}

# ── Build ─────────────────────────────────────────────────────
build() {
    if [ "${SKIP_BUILD:-0}" = "1" ]; then
        warn "SKIP_BUILD=1 — überspringe mvn, vertraue vorhandenen Jars"
        return
    fi

    # quad-core / quad-tool-builtin zuerst: agent-common haengt transitiv
    # daran, also ist die Reihenfolge wichtig.
    if [ ! -d "${QUAD_DIR}" ]; then
        die "quad-quarkus nicht gefunden: ${QUAD_DIR}"
    fi
    info "baue quad-quarkus (quad-core, quad-tool-builtin) ..."
    (cd "${QUAD_DIR}" && mvn -B -ntp clean install -DskipTests -q)

    info "baue kafka-platform (agent-common, orchestrator, worker) ..."
    (cd "${ROOT}" && mvn -B -ntp clean install -DskipTests -q)

    # Beide Images COPYen nur das fertige Fat-Jar — im Container wird nicht
    # gebaut. Deshalb ist kein deps.tar.gz noetig: agent-common und quad-core
    # stecken bereits in BOOT-INF/lib des Worker-Jars.
    for jar in "${ROOT}/agent-orchestrator/target/agent-orchestrator-"*.jar \
               "${ROOT}/quad-workflow-worker/target/quad-workflow-worker-"*.jar; do
        [ -f "${jar}" ] || die "kein Jar gefunden: ${jar}"
    done
    ok "Build fertig"
}

# ── Health-Wait ───────────────────────────────────────────────
wait_healthy() {
    local svc="$1" url="$2" waited=0
    printf "⏳ warte auf %s " "${svc}"
    while [ "${waited}" -lt "${HEALTH_TIMEOUT}" ]; do
        if curl -fsS --max-time 3 "${url}" >/dev/null 2>&1; then
            printf "\n"
            ok "${svc} ist healthy"
            return 0
        fi
        printf "."
        sleep 3
        waited=$((waited + 3))
    done
    printf "\n"
    warn "${svc} nach ${HEALTH_TIMEOUT}s nicht healthy — letzte 40 Logzeilen:"
    compose logs --tail=40 "${svc}" || true
    return 1
}

# ── Befehle ───────────────────────────────────────────────────
do_start() {
    # Vor dem Banner: bei set -u wuerde ein ungesetzter Pflichtwert sonst
    # mit einer rohen Bash-Fehlermeldung abbrechen statt mit unserer.
    check_tools
    check_env

    echo ""
    echo "╔══════════════════════════════════════════════════════╗"
    echo "║   Kafka Minimal: Orchestrator + QUAD-Worker           ║"
    echo "╠══════════════════════════════════════════════════════╣"
    echo "║  Kafka:    ${EXTERNAL_KAFKA_BOOTSTRAP_SERVERS} (extern)   ║"
    echo "║  Postgres: ${POSTGRES_URL}   ║"
    echo "║  Orchestrator: ${ORCH_URL}   ║"
    echo "║  Worker:      ${WORKER_URL}   ║"
    echo "╚══════════════════════════════════════════════════════╝"
    echo ""

    build

    info "images baue ..."
    compose build -q

    info "starte container ..."
    compose up -d

    local failed=0
    wait_healthy orchestrator "${ORCH_URL}" || failed=1
    wait_healthy quad-worker "${WORKER_URL}" || failed=1

    echo ""
    if [ "${failed}" -eq 0 ]; then
        ok "beide services laufen"
        compose ps
        echo ""
        echo "   Logs:    ./start-minimal.sh logs"
        echo "   Stoppen: ./start-minimal.sh stop"
        exit 0
    fi
    echo ""
    warn "mindestens ein service ist nicht healthy — Details siehe Logs oben"
    exit 1
}

case "${1:-start}" in
    start)   do_start ;;
    stop)    compose_placeholders; compose down ;;
    status)  compose_placeholders; compose ps ;;
    logs)    compose_placeholders; compose logs -f ;;
    *)       die "unbekanntes Kommando: $1 (start|stop|status|logs)" ;;
esac