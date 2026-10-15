#!/usr/bin/env bash
# Zaehlt @Test- und @Disabled-Methoden je Testklasse und Schicht.
#
# Die Zahlen in README.md sind eine Momentaufnahme und veralten still: das war
# genau der Befund aus dem Review (README nannte 58 Tests / 2 Befunde, real waren
# es 104 / 3). Dieses Skript erzeugt die Zahl aus dem Quelltext.
#
#   ./scripts/count-tests.sh          # Markdown-Tabelle auf stdout
#   ./scripts/count-tests.sh --check  # Exit 1, wenn die README-Tabelle abweicht
#
# Die README-Tabelle steht zwischen den Markern
#   <!-- test-counts:start --> ... <!-- test-counts:end -->
# --check vergleicht genau diesen Bereich mit der frisch erzeugten Tabelle.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$ROOT/src/test/java"
README="$ROOT/README.md"
START_MARKER='<!-- test-counts:start -->'
END_MARKER='<!-- test-counts:end -->'

layer_of() {
    case "$1" in
        */l0contract/*)    echo "L0 Contract" ;;
        */l1api/*)         echo "L1 API" ;;
        */l2kafka/*)       echo "L2 Kafka" ;;
        */l3e2e/*)         echo "L3 E2E" ;;
        */syscapability/*) echo "Syscapability" ;;
        */support/*)       echo "Support" ;;
        *)                 echo "Other" ;;
    esac
}

# Nur Annotationszeilen zaehlen: `@Disabled` steht auch in Kommentaren
# ("der @Disabled-Test ..."). `\b` verhindert, dass @Test auch @TestFactory zaehlt.
count_of() {
    grep -cE "^[[:space:]]*$1\\b" "$2" 2>/dev/null || true
}

generate() {
    printf '| Layer | Test class | @Test | @Disabled |\n'
    printf '|-------|------------|------:|----------:|\n'
    local total_t=0 total_d=0 t d layer
    while IFS= read -r file; do
        layer="$(layer_of "$file")"
        # support/ enthaelt nur Hilfsklassen, keine Tests.
        [ "$layer" = "Support" ] && continue
        t="$(count_of '@Test' "$file")"
        d="$(count_of '@Disabled' "$file")"
        total_t=$((total_t + t))
        total_d=$((total_d + d))
        printf '| %s | `%s` | %d | %d |\n' "$layer" "${file#"$SRC"/}" "$t" "$d"
    done < <(find "$SRC" -name '*.java' | sort)
    printf '| **Total** |  | **%d** | **%d** |\n' "$total_t" "$total_d"
}

if [ "${1:-}" = "--check" ]; then
    if [ ! -f "$README" ]; then
        echo "count-tests: README nicht gefunden: $README" >&2
        exit 1
    fi
    actual="$(generate)"
    documented="$(sed -n "/$START_MARKER/,/$END_MARKER/p" "$README" | sed '1d;$d')"
    if [ -z "$documented" ]; then
        echo "FEHLER: keine test-counts-Marker in README.md gefunden." >&2
        exit 1
    fi
    if [ "$actual" != "$documented" ]; then
        echo "Die README-Testzahlen weichen vom Quelltext ab:" >&2
        diff <(printf '%s\n' "$documented") <(printf '%s\n' "$actual") || true
        exit 1
    fi
    echo "README-Testzahlen stimmen mit dem Quelltext ueberein."
    exit 0
fi

generate
