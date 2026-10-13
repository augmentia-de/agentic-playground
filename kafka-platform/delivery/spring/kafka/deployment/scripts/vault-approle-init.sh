# ─── Production Vault Pre-Fill Script ──────────────────────
# Creates KV v2 secrets for common paths used by the orchestrator.
# Run this once after initialising Vault (`vault operator init` / `vault login`).
#
# Usage:
#   ./scripts/vault-approle-init.sh <VAULT_ADDR> <ROLE_ID> <SECRET_ID>
#   # Or set VAULT_ADDR & VAULT_TOKEN env vars instead of role_id/secret_id
#
# Environment variables (all optional):
#   VAULT_ADDR     = https://vault.example.com  (default http://localhost:8200)
#   VAULT_TOKEN    = root or token-based auth
#   ROLE_ID        = AppRole role ID
#   SECRET_ID      = AppRole secret ID
#

set -euo pipefail

VAULT_ADDR="${VAULT_ADDR:-http://localhost:8200}"

# ── Auth Method ───────────────────────────────────────────
if [ -n "${VAULT_TOKEN:-}" ]; then
    export VAULT_TOKEN
elif [ -n "${ROLE_ID:-}" ] && [ -n "${SECRET_ID:-}" ]; then
    APPROLE_RESPONSE=$(curl -s -X POST \
        --header "X-Vault-Request: true" \
        -d "{\"role_id\":\"${ROLE_ID}\",\"secret_id\":\"${SECRET_ID}\"}" \
        "${VAULT_ADDR}/v1/auth/approle/login" | jq -r '.auth.client_token')
    if [ -z "$APPROLE_RESPONSE" ] || [ "$APPROLE_RESPONSE" = "null" ]; then
        echo "ERROR: AppRole login failed — check role_id/secret_id"
        exit 1
    fi
    export VAULT_TOKEN="$APPROLE_RESPONSE"
else
    echo "ERROR: Set VAULT_TOKEN or both ROLE_ID + SECRET_ID"
    echo "Usage:"
    echo "  export VAULT_TOKEN=root ..."
    echo "  export ROLE_ID=my-role SECRET_ID=my-secret-id"
    exit 1
fi

echo "Auth succeeded — creating KV v2 paths..."

# ── KV v2 engine paths ───────────────────────────────────
declare -A SECRETS=(
    ["agent-platform/agent-orchestrator/database/password"]="change-me-db-pw"
    ["agent-platform/shared/llm-api-keys/openai-key"]="sk-placeholder-openai"
    ["agent-platform/shared/oauth-secrets/slack-webhook"]="https://hooks.slack.com/placeholer"
    ["agent-platform/shared/oauth-secrets/pagerduty-integration-key"]="PLACEHOLDER-PD-KEY"
    ["agent-platform/agent-orchestrator/redis/password"]="change-me-redis-pw"
)

for path in "${!SECRETS[@]}"; do
    key="${path##*/}"
    value="${SECRETS[$path]}"
    dir="${path%/*}"

    # Create mount point if needed
    curl -s -X POST \
        --header "X-Vault-Token: ${VAULT_TOKEN}" \
        -d '{"type":"kv-v2"}' \
        "${VAULT_ADDR}/v1/sys/mounts/secrets" > /dev/null 2>&1 || true

    # Write secret
    json="{\"data\":{\"${key}\":\"${value}\"}}"
    result=$(curl -s -X PUT \
        --header "X-Vault-Token: ${VAULT_TOKEN}" \
        --header "Content-Type: application/json" \
        -d "$json" \
        "${VAULT_ADDR}/v1/${dir}")

    status=$(echo "$result" | jq -r '.errors // empty' 2>/dev/null)
    if [ -n "$status" ]; then
        echo "  ✗ Failed to write $path — $status"
    else
        echo "  ✓ Created $path"
    fi
done

echo ""
echo "Done. Verify with:"
echo "  vault kv get secret/$path"
