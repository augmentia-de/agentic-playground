#!/usr/bin/env bash
#
# kafka/sasl-ssl-init.sh — Generate self-signed CA, broker keystore/truststore
#                         and client keystores for Kafka SASL_SSL.
#
# Usage:
#   ./kafka/sasl-ssl-init.sh [output-dir]
#
# The generated JKS files are written to ./secrets/ (default).
# After running this script, start Docker Compose with the tls profile:
#   docker compose --profile tls up -d
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SECRETS_DIR="${1:-$SCRIPT_DIR/secrets}"

KEYTOOL="keytool"

if ! command -v "$KEYTOOL" &>/dev/null; then
    # Try common locations
    for dir in /usr/lib/jvm/*/bin; do
        if [ -f "$dir/keytool" ]; then
            KEYTOOL="$dir/keytool"
            break
        fi
    done
fi

if ! command -v "$KEYTOOL" &>/dev/null; then
    echo "ERROR: keytool not found. Install JDK 11+ or set PATH." >&2
    exit 1
fi

mkdir -p "$SECRETS_DIR"

COMMON_NAME="kafka-broker"
KESTORE_PASSWORD="changeit"
VALIDITY_DAYS=3650

echo "=== Step 1: Generate self-signed CA ==="
"$KEYTOOL" -genkeypair \
    -keystore "$SECRETS_DIR/ca.keystore.jks" \
    -keyalg RSA -keysize 4096 \
    -validity "$VALIDITY_DAYS" \
    -dname "CN=Agent Platform CA, OU=Security, O=AgentPlatform, L=, ST=, C=DE" \
    -storepass "$KESTORE_PASSWORD" -keypass "$KESTORE_PASSWORD" \
    -alias ca-key \
    -ext BC:critical=ca:true,pathlen:0 \
    -storetype JKS

"$KEYTOOL" -exportcert \
    -keystore "$SECRETS_DIR/ca.keystore.jks" \
    -storepass "$KESTORE_PASSWORD" \
    -alias ca-key \
    -file "$SECRETS_DIR/ca.crt"

echo "=== Step 2: Create broker keystore + sign with CA ==="
"$KEYTOOL" -genkeypair \
    -keystore "$SECRETS_DIR/kafka.broker.keystore.jks" \
    -keyalg RSA -keysize 2048 \
    -validity "$VALIDITY_DAYS" \
    -dname "CN=$COMMON_NAME, OU=Broker, O=AgentPlatform, L=, ST=, C=DE" \
    -storepass "$KESTORE_PASSWORD" -keypass "$KESTORE_PASSWORD" \
    -alias broker-key

"$KEYTOOL" -certreq \
    -keystore "$SECRETS_DIR/kafka.broker.keystore.jks" \
    -storepass "$KESTORE_PASSWORD" \
    -alias broker-key \
    -file "$SECRETS_DIR/kafka.broker.csr"

"$KEYTOOL" -gencert \
    -keystore "$SECRETS_DIR/ca.keystore.jks" \
    -storepass "$KESTORE_PASSWORD" \
    -alias ca-key \
    -infile "$SECRETS_DIR/kafka.broker.csr" \
    -outfile "$SECRETS_DIR/kafka.broker.signed.crt" \
    -ext SAN=DNS:kafka,DNS:localhost,IP:127.0.0.1 \
    -startdate "0 days ago" \
    -enddate "+${VALIDITY_DAYS}d"

"$KEYTOOL" -importcert \
    -keystore "$SECRETS_DIR/kafka.broker.keystore.jks" \
    -storepass "$KESTORE_PASSWORD" \
    -alias ca-cert \
    -file "$SECRETS_DIR/ca.crt"

"$KEYTOOL" -importcert \
    -keystore "$SECRETS_DIR/kafka.broker.keystore.jks" \
    -storepass "$KESTORE_PASSWORD" \
    -alias broker-key \
    -file "$SECRETS_DIR/kafka.broker.signed.crt" \
    -noprompt

echo "=== Step 3: Create truststore with CA cert ==="
"$KEYTOOL" -importcert \
    -keystore "$SECRETS_DIR/kafka.truststore.jks" \
    -storepass "$KESTORE_PASSWORD" \
    -alias ca-key \
    -file "$SECRETS_DIR/ca.crt" \
    -noprompt

echo "=== Step 4: Create client keystore (optional mTLS) ==="
"$KEYTOOL" -genkeypair \
    -keystore "$SECRETS_DIR/client.keystore.jks" \
    -keyalg RSA -keysize 2048 \
    -validity "$VALIDITY_DAYS" \
    -dname "CN=agent-client, OU=Client, O=AgentPlatform, L=, ST=, C=DE" \
    -storepass "$KESTORE_PASSWORD" -keypass "$KESTORE_PASSWORD" \
    -alias client-key

"$KEYTOOL" -certreq \
    -keystore "$SECRETS_DIR/client.keystore.jks" \
    -storepass "$KESTORE_PASSWORD" \
    -alias client-key \
    -file "$SECRETS_DIR/client.csr"

"$KEYTOOL" -gencert \
    -keystore "$SECRETS_DIR/ca.keystore.jks" \
    -storepass "$KESTORE_PASSWORD" \
    -alias ca-key \
    -infile "$SECRETS_DIR/client.csr" \
    -outfile "$SECRETS_DIR/client.signed.crt"

"$KEYTOOL" -importcert \
    -keystore "$SECRETS_DIR/client.keystore.jks" \
    -storepass "$KESTORE_PASSWORD" \
    -alias ca-cert \
    -file "$SECRETS_DIR/ca.crt"

"$KEYTOOL" -importcert \
    -keystore "$SECRETS_DIR/client.keystore.jks" \
    -storepass "$KESTORE_PASSWORD" \
    -alias client-key \
    -file "$SECRETS_DIR/client.signed.crt" \
    -noprompt

echo ""
echo "=== Generated files in $SECRETS_DIR/ ==="
ls -la "$SECRETS_DIR/"

echo ""
echo "=== Docker Compose startup command ==="
echo "  docker compose --profile tls up -d"
echo ""
echo "Ensure your docker-compose.yml has the kafka service configured"
echo "with KAFKA_OPTS pointing to a JAAS config file that uses these keystores."
