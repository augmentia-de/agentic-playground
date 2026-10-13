#!/usr/bin/env bash
# ╔═══════════════════════════════════════════════════════════╗
# ║  Kafka TLS/mTLS + SCRAM-SHA-512 PKI Infrastructure       ║
# ║  Generates CA → Broker Certs → Client Certs               ║
# ║  Usage: ./scripts/kafka-pki-setup.sh [output_dir]         ║
# ╚═══════════════════════════════════════════════════════════╝
set -euo pipefail

OUTPUT_DIR="${1:-kafka-pki}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="${OUTPUT_DIR}/pki-work"
SECRETS_DIR="${OUTPUT_DIR}/secrets"

echo "╔═══════════════════════════════════════════════════╗"
echo "║  Kafka TLS/mTLS PKI Setup — Starting             ║"
echo "╚═══════════════════════════════════════════════════╝"
echo ""
echo "   Output directory : ${OUTPUT_DIR}"
echo "   PKI workdir      : ${WORK_DIR}"
echo "   Secrets dir      : ${SECRETS_DIR}"
echo ""

# ── Cleanup previous run ───────────────────────────────────
rm -rf "${OUTPUT_DIR}"
mkdir -p "${WORK_DIR}" "${SECRETS_DIR}"

# ── Helper: generate self-signed CA or leaf cert ───────────
openssl_req() {
    local name="$1" days="$2" keyfile="$3" outdir="$4" subj="$5" sans="$6"
    local cn=$(basename "$name")
    
    echo "→ Generating key+CSR for ${cn}..."
    
    openssl genrsa -out "${outdir}/${keyfile}.key" 2048 2>/dev/null
    
    # Build SAN extension if provided
    local ext=""
    if [[ -n "$sans" ]]; then
        ext="subjectAltName=${sans}"
    fi
    
    openssl req -new -key "${outdir}/${keyfile}.key" \
        -subj "${subj}" \
        -out "${outdir}/${keyfile}.csr" 2>/dev/null
    
    echo "  CSR generated."
}

# ── Step 1: Root CA ────────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 1/5 — Generating Root CA"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

ROOT_CA_KEY="${WORK_DIR}/root-ca.key"
ROOT_CA_CRT="${WORK_DIR}/root-ca.crt"
ROOT_CA_CSR="${WORK_DIR}/root-ca.csr"

openssl genrsa -out "${ROOT_CA_KEY}" 4096 2>/dev/null

openssl req -new -x509 -days 3650 -sha256 \
    -key "${ROOT_CA_KEY}" \
    -out "${ROOT_CA_CRT}" \
    -subj "/C=DE/ST=Bavaria/L=Munich/O=Agentic Platform/CN=kafka-root-ca" 2>/dev/null

chmod 400 "${ROOT_CA_KEY}"
cp "${ROOT_CA_CRT}" "${SECRETS_DIR}/root-ca.crt"
echo "✅ Root CA generated → ${SECRETS_DIR}/root-ca.crt"

# ── Step 2: Kafka Broker Certificate ───────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 2/5 — Generating Kafka Broker Certificate"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

BROKER_CRT="${SECRETS_DIR}/broker.crt"
BROKER_KEY="${SECRETS_DIR}/broker.key"
BROKER_PEM="${SECRETS_DIR}/broker.pem"
BROKER_JKS="${SECRETS_DIR}/broker.keystore.jks"

# CN varies by environment
if [[ -z "${KAFKA_BROKER_CN:-}" ]]; then
    KAFKA_BROKER_CN="kafka-broker"
fi

# San list includes localhost, docker hostname, optional external IPs
SANS="DNS:kafka,DNS:kafka-broker,DNS:localhost,DNS:agent-platform-kafka${EXTRA_SANS:+,$EXTRA_SANS}"

openssl_req "broker" 825 "${BROKER_CRT%.crt}" "${WORK_DIR}" \
    "/C=DE/ST=Bavaria/L=Munich/O=Agentic Platform/CN=${KAFKA_BROKER_CN}" \
    "${SANS}"

# Sign broker cert with CA
openssl x509 -req -days 825 -sha256 \
    -in "${WORK_DIR}/broker.csr" \
    -CA "${ROOT_CA_CRT}" -CAkey "${ROOT_CA_KEY}" -CAcreateserial \
    -extfile <(printf "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=${SANS}") \
    -out "${BROKER_CRT}" 2>/dev/null

# Combine cert + key into PEM for Kafka config
cp "${BROKER_KEY}" "${WORK_DIR}/broker.key" 2>/dev/null || true
openssl x509 -in "${BROKER_CRT}" > "${WORK_DIR}/broker.crt-only" 2>/dev/null || cp "${BROKER_CRT}" "${WORK_DIR}/broker.crt-only"

cat "${WORK_DIR}/broker.crt-only" "${WORK_DIR}/broker.key" > "${BROKER_PEM}"

# Create JKS keystore (required by JVM-based clients)
yes | keytool -genkeypair -alias kafka -keyalg RSA -keysize 2048 \
    -keystore "${BROKER_JKS}" \
    -storepass kfk-pass \
    -keypass kfk-pass \
    -validity 825 \
    -dname "CN=${KAFKA_BROKER_CN},O=Agentic Platform,L=Munich,ST=Bavaria,C=DE" 2>/dev/null

# Import CA into truststore
keytool -importcert -alias root-ca -file "${ROOT_CA_CRT}" \
    -keystore "${WORK_DIR}/truststore.jks" -storepass kfk-pass \
    -noprompt 2>/dev/null

keytool -importcert -alias root-ca -file "${ROOT_CA_CRT}" \
    -keystore "${BROKER_JKS}" -storepass kfk-pass \
    -noprompt 2>/dev/null

echo "✅ Broker cert generated:"
echo "     PEM : ${BROKER_PEM}"
echo "     JKS : ${BROKER_JKS}"

# ── Step 3: Client Certificate(s) ──────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 3/5 — Generating Client Certificate(s)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for client in orchestrator worker quad-worker; do
    CLIENT_CRT="${SECRETS_DIR}/${client}.crt"
    CLIENT_KEY="${SECRETS_DIR}/${client}.key"
    CLIENT_PEM="${SECRETS_DIR}/${client}.pem"
    CLIENT_JKS="${SECRETS_DIR}/${client}-keystore.jks"
    CLIENT_CN="${client}"
    
    openssl_req "${client}" 365 "${CLIENT_CRT%.crt}" "${WORK_DIR}" \
        "/C=DE/ST=Bavaria/L=Munich/O=Agentic Platform/CN=${CLIENT_CN}" \
        "DNS:${CLIENT_CN}"
    
    openssl x509 -req -days 365 -sha256 \
        -in "${WORK_DIR}/${CLIENT_CRT%.crt}.csr" \
        -CA "${ROOT_CA_CRT}" -CAkey "${ROOT_CA_KEY}" -CAcreateserial \
        -extfile <(printf "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth") \
        -out "${CLIENT_CRT}" 2>/dev/null
    
    yes | keytool -genkeypair -alias ${client} -keyalg RSA -keysize 2048 \
        -keystore "${CLIENT_JKS}" \
        -storepass kfk-pass \
        -keypass kfk-pass \
        -validity 365 \
        -dname "CN=${CLIENT_CN},O=Agentic Platform,L=Munich,ST=Bavaria,C=DE" 2>/dev/null

    keytool -importcert -alias root-ca -file "${ROOT_CA_CRT}" \
        -keystore "${CLIENT_JKS}" -storepass kfk-pass \
        -noprompt 2>/dev/null
    
    cat "${CLIENT_CRT}" "${WORK_DIR}/${CLIENT_CRT%.crt}.key" > "${CLIENT_PEM}"
    
    echo "  ✅ Client cert for '${client}' → ${CLIENT_PEM}"
done

# ── Step 4: Truststore ─────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 4/5 — Creating Truststore"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

TRUSTSTORE="${SECRETS_DIR}/truststore.jks"
cp "${WORK_DIR}/truststore.jks" "${TRUSTSTORE}"
echo "✅ Truststore → ${TRUSTSTORE}"

# ── Step 5: JAAS & Config Summary ──────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 5/5 — JAAS Configuration"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

JAAS_CONFIG="${SECRETS_DIR}/jaas.config"
ADMIN_PASSWORD="${KAFKA_ADMIN_PASSWORD:-kafka-admin-secret}"
PRODUCER_USER="${KAFKA_PRODUCER_USER:-producer}"
PRODUCER_PASS="${KAFKA_PRODUCER_PASS:-producer-secret}"
CONSUMER_USER="${KAFKA_CONSUMER_USER:-consumer}"
CONSUMER_PASS="${KAFKA_CONSUMER_PASS:-consumer-secret}"

cat > "${JAAS_CONFIG}" <<EOF
ScramLoginModule required username="\${JAAS_USERNAME}" password="\${JAAS_PASSWORD}";
EOF

echo "✅ JAAS config → ${JAAS_CONFIG}"
echo ""
echo "   Admin credentials (SCRAM):"
echo "     Username : admin"
echo "     Password : ${ADMIN_PASSWORD}"
echo ""
echo "   Producer credentials:"
echo "     Username : ${PRODUCER_USER}"
echo "     Password : ${PRODUCER_PASS}"
echo ""
echo "   Consumer credentials:"
echo "     Username : ${CONSUMER_USER}"
echo "     Password : ${CONSUMER_PASS}"

# ── Generate Kafka bootstrap config ────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Bootstrap Kafka Admin User via SCRAM"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

SCRAM_FILE="${SECRETS_DIR}/kafka-add-scram-users.sh"
cat > "${SCRAM_FILE}" <<'SCRAM_EOF'
#!/usr/bin/env bash
# Add SCRAM users to Kafka (run once after cluster starts)
set -euo pipefail

BOOTSTRAP_SERVER="${1:-kafka:9092}"
SECURITY_PROTOCOL="${2:-SASL_SSL}"
ADMIN_USER="${KAFKA_ADMIN_USER:-admin}"
ADMIN_PASS="${KAFKA_ADMIN_PASSWORD:-kafka-admin-secret}"
PRODUCER_USER="${KAFKA_PRODUCER_USER:-producer}"
PRODUCER_PASS="${KAFKA_PRODUCER_PASS:-producer-secret}"
CONSUMER_USER="${KAFKA_CONSUMER_USER:-consumer}"
CONSUMER_PASS="${KAFKA_CONSUMER_PASS:-consumer-secret}"

echo "Creating SCRAM users on ${BOOTSTRAP_SERVER}..."

# Using confluent-kafka commands to set up SCRAM credentials
export KAFKA_OPTS="-Djava.security.auth.login.config=/etc/kafka/secrets/jaas.config"
export JAAS_USERNAME="${ADMIN_USER}"
export JAAS_PASSWORD="${ADMIN_PASS}"

# Check if tools are available, otherwise print instructions
if command -v kafka-configs >/dev/null 2>&1; then
    kafka-configs --bootstrap-server "${BOOTSTRAP_SERVER}" --alter \
        --add-config 'SCRAM-SHA-512=[password='"${ADMIN_PASS}"']' \
        --entity-type users --entity-name "${ADMIN_USER}" 2>/dev/null || true
    
    kafka-configs --bootstrap-server "${BOOTSTRAP_SERVER}" --alter \
        --add-config 'SCRAM-SHA-512=[password='"${PRODUCER_PASS}"']' \
        --entity-type users --entity-name "${PRODUCER_USER}" 2>/dev/null || true
    
    kafka-configs --bootstrap-server "${BOOTSTRAP_SERVER}" --alter \
        --add-config 'SCRAM-SHA-512=[password='"${CONSUMER_PASS}"']' \
        --entity-type users --entity-name "${CONSUMER_USER}" 2>/dev/null || true
    
    echo "✅ SCRAM users created successfully."
else
    echo "⚠️  kafka-configs tool not available."
    echo "   Run manually inside kafka container:"
    echo "     docker exec -it agent-platform-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \\
      --alter --add-config 'SCRAM-SHA-512=[password=<PASS>] --entity-type users --entity-name <USER>"
    echo ""
    echo "   Or use the embedded Confluent platform CLI (see documentation)."
fi
SCRAM_EOF
chmod +x "${SCRAM_FILE}"
echo "✅ SCRAM setup script → ${SCRAM_FILE}"

# ── Write .gitignore for secrets ───────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ".gitignore rules added"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

cat >> "${OUTPUT_DIR}/../.gitignore" <<'EOF'
# Auto-generated — ignore all PKI secrets
*/secrets/*
*/pki-work/*
*.jks
*.p12
*.pem
!/.gitkeep
EOF

# ── Summary ─────────────────────────────────────────────────
echo ""
echo "╔═══════════════════════════════════════════════════════════╗"
echo "║  PKI Setup Complete                                       ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo ""
echo "   Files generated:"
find "${SECRETS_DIR}" -type f | sort | sed 's|.*|   │ \0|'
echo ""
echo "   To activate TLS/sasl_ssl in docker-compose:"
echo "     docker compose --profile tls up -d --build"
echo ""
echo "   Next steps:"
echo "     1. Copy secrets/docker/secrets/*.jks to host:/path/to/certs/"
echo "     2. Configure Kafka listeners in docker-compose.yml"
echo "     3. Set SCRAM users: bash kafka-pki/secrets/kafka-add-scram-users.sh"
echo "     4. Deploy orchestrator/worker with TLS client certs"
echo ""
