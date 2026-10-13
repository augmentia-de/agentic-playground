#!/usr/bin/env bash
# Erzeugt und erneuert die Delivery-Struktur aus den Quell-Repos.
#
#   ./sync-from-source.sh
#
# Kopiert ausschliesslich git-getrackte Dateien. Dadurch kann keine .env, kein
# .env.secrets.* und nichts aus docker/secrets/ in die Delivery geraten - genau
# die Dateien, die in beiden Repos gitignored sind. Das ist keine Vorsicht
# nebenbei, sondern der Grund fuer diesen Weg: eine Delivery wird weitergegeben,
# und ein versehentlich mitgelieferter Schluessel waere afterwards nicht mehr
# aus dem Working Tree zu entfernen.
#
# Das Mapping steht ausschliesslich hier. Ordner und Skript koennen dadurch nicht
# auseinanderlaufen; nach einer Aenderung an den Quell-Repos genuegt ein Lauf.
#   ./sync-from-source.sh            erneuert die kopierten Dateien
#   ./sync-from-source.sh --clean    leert vorher die generierten Ordner
#
# Ohne --clean arbeitet das Skript additiv und lässt README.md stehen. --clean
# entfernt nur deployment/ und config/, weil dort ausschliesslich Kopien liegen.
# Ein volles rm -rf waere falsch: die README.md sind von Hand gepflegt und nicht
# Teil des Mappings, sie waeren beim naechsten Lauf weg.
#
# --clean behebt zugleich das Drift-Problem eines additiven Laufs: eine Datei, die
# im Quell-Repo geloescht wurde, bleibt sonst als veraltete Kopie liegen.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"

if [ "${1:-}" = "--clean" ]; then
  echo "== --clean: generierte Ordner leeren (README.md bleiben) =="
  for runtime in quarkus spring; do
    for app in quad kafka; do
      for gen in deployment config; do
        d="$HERE/$runtime/$app/$gen"
        if [ -d "$d" ]; then
          echo "  entferne ${runtime}/${app}/${gen}"
          rm -rf "$d"
        fi
      done
    done
  done
fi
SELF="$(cd "$HERE/.." && pwd)"

# Die beiden Repos liegen als Geschwister nebeneinander, die Delivery liegt in einem
# davon. Also erst selbst suchen, dann den Parent als Fallback.
find_repo() {
  local name="$1" candidates=("$SELF" "$SELF/.." "$(dirname "$SELF")")
  for base in "${candidates[@]}"; do
    if [ -d "$base/$name" ]; then
      (cd "$base/$name" && pwd)
      return 0
    fi
  done
  return 1
}

QUAD="$(find_repo quad-quarkus)" || { echo "FEHLER: quad-quarkus nicht neben $SELF gefunden" >&2; exit 1; }
KAFKA="$(find_repo kafka-platform)" || { echo "FEHLER: kafka-platform nicht neben $SELF gefunden" >&2; exit 1; }

echo "Quelle quad:  $QUAD"
echo "Quelle kafka: $KAFKA"
echo
copied=0
skipped=0

# copy <repo> <quelle-relativ> <ziel-relativ>
copy() {
  local repo="$1" src="$2" dst="$HERE/$3"
  if git -C "$repo" ls-files --error-unmatch "$src" >/dev/null 2>&1; then
    mkdir -p "$(dirname "$dst")"
    cp "$repo/$src" "$dst"
    copied=$((copied + 1))
  else
    echo "  uebersprungen (untracked): $src"
    skipped=$((skipped + 1))
  fi
}

# Verzeichnis rekursiv, aber nur tracked Dateien
copy_tree() {
  local repo="$1" dir="$2" dst="$3"
  local found=0
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    copy "$repo" "$f" "$dst/${f#"$dir"/}"
    found=1
  done < <(git -C "$repo" ls-files "$dir")
  [ "$found" = 1 ] || echo "  Hinweis: keine tracked Dateien unter $dir"
}

echo "== delivery/quarkus/quad  (Quarkus-Laufzeit, quad-Anwendung) =="
copy "$QUAD" "docker/backend.Dockerfile"                 "quarkus/quad/deployment/docker/backend.Dockerfile"
copy "$QUAD" "deploy/05-app-quarkus.yml"                 "quarkus/quad/deployment/compose/05-app-quarkus.yml"
copy "$QUAD" "deploy/05-app-minimal.yml"                 "quarkus/quad/deployment/compose/05-app-minimal.yml"
copy "$QUAD" "deploy/nginx-minimal.conf"                 "quarkus/quad/deployment/nginx-minimal.conf"
copy "$QUAD" "scripts/build.sh"                          "quarkus/quad/deployment/scripts/build.sh"
copy "$QUAD" "scripts/start.sh"                          "quarkus/quad/deployment/scripts/start.sh"
copy_tree "$QUAD" "deploy/helm/quad"                      "quarkus/quad/deployment/helm"
copy "$QUAD" ".env.example"                              "quarkus/quad/config/env/.env.example"
copy "$QUAD" "config/mcp-config.json"                    "quarkus/quad/config/mcp-config.json"
copy "$QUAD" "deploy/.env.secrets.cloud.example"         "quarkus/quad/config/env/.env.secrets.cloud.example"
[ -f "$QUAD/deploy/.env.prod" ] && cp "$QUAD/deploy/.env.prod" "$HERE/quarkus/quad/config/env/.env.prod.example" && copied=$((copied+1))

echo "== delivery/quarkus/kafka  (Kafka-Infrastruktur des Quarkus-Stacks) =="
copy "$QUAD" "deploy/01-infrastructure.yml"              "quarkus/kafka/deployment/compose/01-infrastructure.yml"
copy "$QUAD" "deploy/prometheus.yml"                     "quarkus/kafka/config/observability/prometheus.yml"
copy "$QUAD" "deploy/otel-collector-config.yaml"         "quarkus/kafka/config/observability/otel-collector-config.yaml"
copy "$QUAD" "deploy/tempo.yaml"                         "quarkus/kafka/config/observability/tempo.yaml"
copy "$KAFKA" "kafka/sasl-ssl-init.sh"                   "quarkus/kafka/deployment/scripts/sasl-ssl-init.sh"
copy "$KAFKA" "scripts/kafka-pki-setup.sh"               "quarkus/kafka/deployment/scripts/kafka-pki-setup.sh"

echo "== delivery/spring/quad  (Spring-Boot-Laufzeit, quad-workflow-worker) =="
copy "$KAFKA" "docker/Dockerfile-quad-workflow-worker"         "spring/quad/deployment/docker/Dockerfile-quad-workflow-worker"
copy "$KAFKA" "docker/Dockerfile-quad-workflow-worker-runtime"  "spring/quad/deployment/docker/Dockerfile-quad-workflow-worker-runtime"
copy "$KAFKA" "scripts/build-deps-tarball.sh"                   "spring/quad/deployment/scripts/build-deps-tarball.sh"
# docker-compose.minimal.yml und start-minimal.sh starten Orchestrator UND Worker.
# Sie gehoeren deshalb nicht unter spring/quad, sondern als plattformweiter Stack
# unter spring/kafka - die Aufteilung nach App wuerde hier etwas behaupten, das die
# Dateien nicht hergeben.
mkdir -p "$HERE/spring/kafka/config/helm"
cat > "$HERE/spring/kafka/config/helm/values.delivery.yaml" <<'VALEOF'
# Overlay fuer values.yaml aus deployment/helm. Nur die Werte, die fuer eine
# Auslieferung gesetzt werden muessen; alles andere bleibt beim Chart-Default.
#
#   helm install agent-platform deployment/helm \
#     -f config/helm/values.delivery.yaml -f meine-umgebung.yaml
#
# Der Chart-Default startet Kafka und Postgres mit je 1 Replika und festen
# Passwoertern. Beides ist fuer eine Auslieferung zu ersetzen.

image:
  orchestrator:
    repository: agent-platform/orchestrator
    tag: "1.0.0-SNAPSHOT"
  worker:
    repository: agent-platform/worker
    tag: "1.0.0-SNAPSHOT"

orchestrator:
  replicaCount: 2

worker:
  minReplicas: 2
  maxReplicas: 20

kafka:
  # Bei externem Kafka: false setzen und externalBootstrapServers fuellen.
  # Sonst startet der Chart einen eigenen Broker mit einem Replikat.
  replicas: 3
  externalBootstrapServers: ""
  clientProtocol: SASL_SSL
  saslMechanism: SCRAM-SHA-512

postgres:
  enabled: true
  postgresqlDatabase: agent_platform
  postgresqlUsername: agent
  # Pflicht: durch Secret-Referenz ersetzen, nicht hier belassen.
  postgresqlPassword: ""
VALEOF
copied=$((copied + 1))

mkdir -p "$HERE/spring/quad/config"
cat > "$HERE/spring/quad/config/worker.env.example" <<'ENVEOF'
# quad-workflow-worker: erforderliche Einstellungen fuer importierte Agenten.
# Als docker-compose-Umgebungsvariablen bzw. Container-Env verwenden.
#
# agent-source=db ist Pflicht fuer alles, was ueber /api/v1/unified/agents/convert
# importiert wurde. Ohne diese Einstellung kennt der Worker nur die statische
# YAML-Liste und meldet "Unknown agent type" fuer jeden importierten Agenten.
WORKER_QUAD_AGENT_SOURCE=db

# Ohne Key faellt der Worker auf ein Mock-Modell zurueck.
OPENAI_API_KEY=
OPENAI_BASE_URL=https://openrouter.ai/api/v1
OPENAI_MODEL=

# Der Worker registriert genau EINEN Model-Key: "simple" (aus OPENAI_*).
# Der modelId eines importierten Agenten muss genau dieser Key sein.
# app.agents.default-model-key auf der Orchestrator-Seite muss dazu passen.

# Der Listener blockiert den Poll-Thread bis zum Ende des Batches. Der Task-Timeout
# muss darunter bleiben, sonst Rebalance und doppelte LLM-Aufrufe.
# worker.quad.kafka.max-task-timeout-ms (600000) < max.poll.interval.ms (900000)
ENVEOF
copied=$((copied + 1))

echo "== delivery/spring/kafka  (Kafka-Plattform, Spring Boot) =="
copy "$KAFKA" "docker/Dockerfile-agent-orchestrator"    "spring/kafka/deployment/docker/Dockerfile-agent-orchestrator"
copy "$KAFKA" "docker/Dockerfile-agent-worker"          "spring/kafka/deployment/docker/Dockerfile-agent-worker"
copy "$KAFKA" "docker-compose.yml"                      "spring/kafka/deployment/compose/docker-compose.yml"
copy "$KAFKA" "docker/compose-extensions.yml"           "spring/kafka/deployment/compose/compose-extensions.yml"
copy "$KAFKA" "docker-compose.minimal.yml"              "spring/kafka/deployment/compose/docker-compose.minimal.yml"
copy "$KAFKA" "docker-compose.two-workers.yml"          "spring/kafka/deployment/compose/docker-compose.two-workers.yml"
copy "$KAFKA" "start-minimal.sh"                        "spring/kafka/deployment/scripts/start-minimal.sh"
copy "$KAFKA" "scripts/e2e-test.sh"                     "spring/kafka/deployment/scripts/e2e-test.sh"
copy "$KAFKA" "scripts/vault-approle-init.sh"           "spring/kafka/deployment/scripts/vault-approle-init.sh"
copy_tree "$KAFKA" "helm"                               "spring/kafka/deployment/helm"
# values.yaml gehoert zum Chart und liegt dort bereits. Hier kein zweites Exemplar -
# zwei Kopien driften auseinander und man pflegt die falsche. Stattdessen ein Overlay
# mit den Werten, die fuer eine Auslieferung gesetzt werden muessen.
copy "$KAFKA" "docker/otel-config.yaml"                 "spring/kafka/config/observability/otel-config.yaml"
copy "$KAFKA" "prometheus/prometheus.yml"               "spring/kafka/config/observability/prometheus.yml"
copy_tree "$KAFKA" "grafana"                             "spring/kafka/config/observability/grafana"

find "$HERE" -name '*.sh' -exec chmod +x {} + 2>/dev/null || true

echo
echo "fertig: $copied Dateien kopiert, $skipped uebersprungen"
