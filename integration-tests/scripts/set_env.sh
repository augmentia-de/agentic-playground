#!/usr/bin/env bash
# Umgebung fuer die Integrationstests setzen.
#
#   . ./scripts/set_env.sh          # im Skript-Verzeichnis oder mit Pfad
#
# Bewusst zum Einbinden (source), nicht zum Aufrufen: nur so exportieren die
# Variablen in die aufrufende Shell. Als eigenes Skript aufgerufen waeren sie nach
# dem Aufruf weg — der Worker laeuft dann im Mock und man wundert sich ueber
# konstante Konfidenzwerte von 0.95.
#
# Reihenfolge der Herkunft, spaeter gewinnt:
#   1. Vorgaben in diesem Skript          (Infrastruktur, Ports, Defaults)
#   2. set_env.local.sh                   (echte Schluessel, liegt in .gitignore)
#   3. was in deiner Shell schon steht    (exportiert vor dem Aufruf)
#
# Der zweite Punkt ist der wichtige: hier steht kein geheimer Wert, sondern nur der
# Ort, an dem einer hingeholt wird. Sonst waere genau die Datei, die beim
# Zufuegen des Skripts im Repo landet, auch die mit dem Schluessel darin.

# ── Infrastruktur ───────────────────────────────────────────────────────────
# Muss laufen, sonst bricht der Runner ab. Er startet sie nicht selbst.

export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export POSTGRES_URL="${POSTGRES_URL:-jdbc:postgresql://localhost:5432/ap}"

# Redis wird nicht geprueft. Ohne sie faellt nur das Auto-Scaling aus; die
# Workflows laufen. Deshalb ist sie hier kein harter Fehler.
export REDIS_HOST="${REDIS_HOST:-localhost}"
export REDIS_PORT="${REDIS_PORT:-6379}"

# ── Ports ────────────────────────────────────────────────────────────────────
# Bewusst ausserhalb der Standardports, damit ein laufender Entwickler-Stack
# nicht mitgetroffen wird.

export ORCH_PORT="${ORCH_PORT:-18080}"
export WORKER_PORT="${WORKER_PORT:-18081}"
export QUAD_PORT="${QUAD_PORT:-18086}"

# Wo der Runner Logs und Pid-Dateien ablegt.
export RUN_DIR="${RUN_DIR:-/tmp/opencode/run/it}"

# ── LLM ──────────────────────────────────────────────────────────────────────
# Ohne OPENAI_API_KEY laeuft der Worker im Mock-Modus. Das ist kein Fehler und
# fuer die meisten Tests richtig: der Mock antwortet auf jeden Prompt mit
# {"confidence": 0.95, "status": "COMPLETED"}, wodurch die Verzweigungen erst
# deterministisch werden. Es heisst aber auch: Timeout-, Retry- und
# Eskalationspfade sind im Mock nicht erreichbar — der Worker laeuft nie in
# einen Fehler.

export OPENAI_API_KEY="${OPENAI_API_KEY:-}"
export OPENAI_MODEL="${OPENAI_MODEL:-deepseek/deepseek-v4-flash-0731}"
export OPENAI_BASE_URL="${OPENAI_BASE_URL:-https://openrouter.ai/api/v1}"
export LLM_MAX_TOKENS="${LLM_MAX_TOKENS:-4096}"
export LLM_TIMEOUT_SECONDS="${LLM_TIMEOUT_SECONDS:-660}"
export LLM_TEMPERATURE="${LLM_TEMPERATURE:-0.2}"

# Worker liest die Agent-Konfiguration aus der gemeinsamen Datenbank statt aus
# seiner YAML. Ohne das kommen die Iterationsgrenzen und Agent-Prompts nicht an,
# obwohl der Orchestrator sie hineingeschrieben hat.
export WORKER_QUAD_AGENT_SOURCE="${WORKER_QUAD_AGENT_SOURCE:-db}"

# ── Lokale Zugangsdaten ──────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOKALE_DATEN="$SCRIPT_DIR/set_env.local.sh"

if [[ -f "$LOKALE_DATEN" ]]; then
    # shellcheck disable=SC1090
    . "$LOKALE_DATEN"
    echo "==> Lokale Zugangsdaten geladen: $LOKALE_DATEN"
elif [[ -f "$SCRIPT_DIR/set_env.local.sh.example" ]]; then
    echo "==> Keine set_env.local.sh gefunden."
    echo "    Zum Ausfuellen:  cp $SCRIPT_DIR/set_env.local.sh.example $LOKALE_DATEN"
fi

# ── Kurzbericht ──────────────────────────────────────────────────────────────
# Sagt, woran es liegt, statt beim ersten fehlgeschlagenen Test zu raten.

set_env_pruefe_und_melde() {
    local fehler=0

    if ! command -v nc >/dev/null 2>&1; then
        echo "(nc fehlt — Erreichbarkeit wurde nicht geprueft)"
    else
        # jdbc:postgresql://host:port/db -> host:port
        local pg_hostport="${POSTGRES_URL#jdbc:postgresql://}"
        pg_hostport="${pg_hostport%%/*}"
        # Sonst steht am Ende noch "/ap" dran
        pg_hostport="${pg_hostport%/*}"

        nc -z "${KAFKA_BOOTSTRAP_SERVERS%%:*}" "${KAFKA_BOOTSTRAP_SERVERS##*:}" 2>/dev/null \
            || { echo "!! Kafka antwortet nicht auf $KAFKA_BOOTSTRAP_SERVERS" >&2; fehler=1; }

        nc -z "${pg_hostport%%:*}" "${pg_hostport##*:}" 2>/dev/null \
            || { echo "!! Postgres antwortet nicht auf $pg_hostport" >&2; fehler=1; }
    fi

    if [[ -z "$OPENAI_API_KEY" ]]; then
        echo "==> Kein OPENAI_API_KEY: der Worker laeuft im Mock-Modus."
        echo "    Das ist fuer die meisten Tests richtig, heisst aber:"
        echo "    Konfidenz immer 0.95, keine Timeout-, Retry- oder Eskalationspfade."
    else
        echo "==> LLM konfiguriert: $OPENAI_MODEL ueber $OPENAI_BASE_URL"
    fi

    if [[ -z "${OPENAI_API_KEY}" && "${USE_ECHTES_LLM:-0}" == "1" ]]; then
        echo "!! USE_ECHTES_LLM=1 verlangt einen Schluessel, aber OPENAI_API_KEY ist leer." >&2
        fehler=1
    fi

    echo "==> Ports: Orchestrator $ORCH_PORT, Worker $WORKER_PORT, quad $QUAD_PORT"
    echo "==> Logs:  $RUN_DIR"

    return $fehler
}