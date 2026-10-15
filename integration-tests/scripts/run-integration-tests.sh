#!/usr/bin/env bash
# Startet den kompletten Stack und fuehrt die Integrationstests aus.
#
#   ./scripts/run-integration-tests.sh          L0+L1+L2 (Broker + Orchestrator noetig)
#   ./scripts/run-integration-tests.sh e2e      alles, zusaetzlich quad + Worker
#   ./scripts/run-integration-tests.sh --start  Stack starten, nichts testen
#   ./scripts/run-integration-tests.sh --stop   Stack stoppen
#
# Ports sind bewusst ausserhalb der Standardports, damit ein laufender Entwickler-Stack
# nicht mitgetroffen wird. Alle ueber Systemeigenschaften ueberschreibbar.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(cd "$ROOT/.." && pwd)"
RUN_DIR="${RUN_DIR:-/tmp/opencode/run/it}"

ORCH_PORT="${ORCH_PORT:-18080}"
QUAD_PORT="${QUAD_PORT:-18086}"
WORKER_PORT="${WORKER_PORT:-18081}"
KAFKA="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
POSTGRES_URL="${POSTGRES_URL:-jdbc:postgresql://localhost:5432/ap}"

ORCH_JAR="$REPO/kafka-platform/agent-orchestrator/target/agent-orchestrator-1.0.0-SNAPSHOT.jar"
WORKER_JAR="$REPO/kafka-platform/quad-workflow-worker/target/quad-workflow-worker-1.0.0-SNAPSHOT.jar"
QUAD_RUN="$REPO/quad-quarkus/quad-quarkus/target/quarkus-app/quarkus-run.jar"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m==> %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31m==> %s\033[0m\n' "$*" >&2; exit 1; }

wait_for() { # url, seconds, name
  local url=$1 secs=$2 name=$3 i
  for ((i = 0; i < secs; i++)); do
    if curl -fsS -o /dev/null "$url" 2>/dev/null; then return 0; fi
    sleep 1
  done
  die "$name antwortet nach ${secs}s nicht auf $url"
}

# ── Stack ────────────────────────────────────────────────────────────────────

start_kafka() {
  if nc -z "${KAFKA%%:*}" "${KAFKA##*:}" 2>/dev/null; then
    log "Kafka läuft bereits auf $KAFKA"
  else
    die "kein Broker auf $KAFKA. Der Runner startet keinen — siehe README, Abschnitt Infrastruktur."
  fi
}

start_orchestrator() {
  [[ -f "$ORCH_JAR" ]] || die "Orchestrator-JAR fehlt: $ORCH_JAR
  baue es mit:  (cd $REPO/kafka-platform && mvn -o -DskipTests package)"
  mkdir -p "$RUN_DIR"
  log "Orchestrator auf :$ORCH_PORT"
  SERVER_PORT="$ORCH_PORT" \
  POSTGRES_URL="$POSTGRES_URL" \
  KAFKA_BOOTSTRAP_SERVERS="$KAFKA" \
    setsid nohup java -jar "$ORCH_JAR" \
      --quarkus.base-url="http://localhost:$QUAD_PORT" \
      > "$RUN_DIR/orch.log" 2>&1 < /dev/null &
  echo $! > "$RUN_DIR/orch.pid"
  wait_for "http://localhost:$ORCH_PORT/actuator/health" 120 Orchestrator
}

start_worker() {
  [[ -f "$WORKER_JAR" ]] || die "Worker-JAR fehlt: $WORKER_JAR
  baue ihn mit: (cd $REPO/kafka-platform && mvn -o -DskipTests package)"
  log "Worker auf :$WORKER_PORT"
  WORKER_GROUP_ID="${WORKER_GROUP_ID:-agent-worker-group}" \
  KAFKA_BOOTSTRAP_SERVERS="$KAFKA" \
    setsid nohup java -jar "$WORKER_JAR" \
      --server.port="$WORKER_PORT" \
      > "$RUN_DIR/worker.log" 2>&1 < /dev/null &
  echo $! > "$RUN_DIR/worker.pid"
  wait_for "http://localhost:$WORKER_PORT/actuator/health" 120 Worker
}

start_quad() {
  [[ -f "$QUAD_RUN" ]] || die "quarkus-run.jar fehlt: $QUAD_RUN
  baue es mit: (cd $REPO/quad-quarkus && mvn -o -DskipTests install)"
  # quad legt seine SQLite-Datei als data/quad.db an — relativ zum Arbeitsverzeichnis,
  # nicht zum Jar. Deshalb muss der Prozess dort starten, sonst findet Flyway die Datei nicht.
  mkdir -p "$RUN_DIR/quad/data"
  log "quad auf :$QUAD_PORT (Daten in $RUN_DIR/quad)"
  (
    cd "$RUN_DIR/quad"
    QUARKUS_HTTP_PORT="$QUAD_PORT" exec setsid nohup java -jar "$QUAD_RUN" \
      > "$RUN_DIR/quad.log" 2>&1 < /dev/null
  ) &
  echo $! > "$RUN_DIR/quad.pid"
  wait_for "http://localhost:$QUAD_PORT/q/health" 120 quad
}

stop_stack() {
  for pidfile in "$RUN_DIR"/*.pid; do
    [[ -f "$pidfile" ]] || continue
    local pid; pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then
      log "stoppe PID $pid ($(basename "$pidfile" .pid))"
      kill "$pid" 2>/dev/null || true
      for _ in {1..20}; do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$pidfile"
  done
}

# ── Tests ────────────────────────────────────────────────────────────────────

run_tests() { # profile
  local profile=$1
  log "Integrationstests (Profil: $profile) — $ROOT"
  # shellcheck disable=SC2046
  ( cd "$ROOT" && mvn -o test -P"$profile" \
      -Dit.orchestrator.url="http://localhost:$ORCH_PORT" \
      -Dit.quad.url="http://localhost:$QUAD_PORT" \
      -Dit.kafka.bootstrap="$KAFKA" \
      -Dit.worker.url="http://localhost:$WORKER_PORT" \
      -Dit.orchestrator.log="$RUN_DIR/orch.log" )
}

# ── Ablauf ───────────────────────────────────────────────────────────────────

case "${1:-integration}" in
  --start)
    start_kafka; start_orchestrator; start_worker; start_quad
    log "Stack laeuft. Logs: $RUN_DIR"
    ;;
  --stop)
    stop_stack
    ;;
  integration)
    start_kafka; start_orchestrator
    trap stop_stack EXIT
    run_tests integration
    ;;
  e2e)
    start_kafka; start_orchestrator; start_worker; start_quad
    trap stop_stack EXIT
    run_tests e2e
    ;;
  *)
    die "unbekanntes Argument '$1' — erwartet: integration | e2e | --start | --stop"
    ;;
esac