# ╔═══════════════════════════════════════════════════════╗
# ║  Kafka SASL_SSL — Production Security Layer          ║
# ║  TLS/mTLS + SCRAM-SHA-512 Authentication             ║
# ╚═══════════════════════════════════════════════════════╝

## Quick Start

```bash
# 1. Generate full PKI infrastructure (CA → broker certs → client certs)
./scripts/kafka-pki-setup.sh kafka-pki

# 2. Set environment variables for SCRAM credentials
export KAFKA_ADMIN_USER=admin
export KAFKA_ADMIN_PASSWORD=kafka-admin-secret
export KAFKA_PRODUCER_USER=producer
export KAFKA_PRODUCER_PASS=producer-secret
export KAFKA_CONSUMER_USER=consumer
export KAFKA_CONSUMER_PASS=consumer-secret

# 3. Copy generated certificates to docker compose secrets directory
mkdir -p ./secrets
cp -r ./kafka-pki/secrets/* ./secrets/

# 4. Deploy with TLS profile
docker compose --profile tls up -d --build

# 5. Initialize SCRAM users on Kafka
bash ./kafka-pki/secrets/kafka-add-scram-users.sh localhost:9092 SASL_SSL
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│              Client Applications                     │
│   Orchestrator  │  Agent Worker  │  Quad Worker     │
└──────┬──┬──┬────────────────────────────────────────┘
       │  │  │
       ▼  ▼  ▼
┌───────────────────────────────────────────────┐
│      TLS/TCP Transport Layer                  │
│  • SSL Certificate Validation                 │
│  • Mutual TLS (client auth: required)         │
│  • Broker keystore & truststore               │
└──────────────────┬────────────────────────────┘
                   │
┌──────────────────▼────────────────────────────┐
│      SASL/SCRAM-SHA-512 Authentication        │
│  • Admin user: admin                          │
│  • Producer user: producer                    │
│  • Consumer group: agent-worker-group         │
└──────────────────┬────────────────────────────┘
                   │
┌──────────────────▼────────────────────────────┐
│       Apache Kafka (SASL_SSL listeners)       │
│  Port: 9092                                   │
└───────────────────────────────────────────────┘
```

## Components

### 1. PKI Infrastructure (`kafka-pki-setup.sh`)

Generates a complete PKI:
- **Root CA** — self-signed, 10-year validity
- **Broker certificate** — signed by CA, SAN includes all Kafka hostnames
- **Client certificates** — for orchestrator, worker, quad-worker
- **Truststore/JKS** — JVM-compatible keystores for each component

Output structure:
```
kafka-pki/
├── secrets/                           # Generated keys & certificates
│   ├── root-ca.crt                    # Root CA certificate
│   ├── broker.crt / .key / .pem       # Broker identity
│   ├── broker.keystore.jks            # JVM keystore (broker)
│   ├── orchestrator.crt/.key/.pem     # Orchestrator client cert
│   ├── worker.crt/.key/.pem           # Worker client cert
│   ├── truststore.jks                 # Trust store (all clients)
│   └── jaas.config                    # JAAS login config template
└── pki-work/                          # Working directory (intermediate files)
```

Usage:
```bash
# Default output to ./kafka-pki/
./scripts/kafka-pki-setup.sh

# Custom output directory
./scripts/kafka-pki-setup.sh ~/my-kafka-security
```

### 2. Docker Compose Profiles

Two configurations available:

#### Development (Plaintext)
```bash
docker compose up -d
```
- Single PLAINTEXT listener on port 9092
- No authentication or encryption
- Suitable only for local development

#### Production (TLS + SCRAM)
```bash
# 1. Run the PKI setup first
./scripts/kafka-pki-setup.sh kafka-pki

# 2. Move secrets into place
mkdir -p ./secrets && cp ./kafka-pki/secrets/* ./secrets/

# 3. Start with TLS profile
docker compose --profile tls up -d --build
```

Services included in TLS profile:
- `kafka-tls` — Kafka with SASL_SSL listeners
- `orchestrator` — with TLS client settings
- `worker` — with TLS client settings
- `jaeger` — distributed tracing
- `prometheus` — metrics scraping
- `grafana` — dashboarding

### 3. Spring Boot Configuration

#### `application-tls.yaml` (in project root)
Active when `SPRING_PROFILES_ACTIVE=tls`:

| Setting | Value | Description |
|---------|-------|-------------|
| `spring.kafka.producer.bootstrap-servers` | `tls://kafka-tls:9092` | Kafka bootstrap URL |
| `security.protocol` | `SASL_SSL` | TLS transport + SCRAM auth |
| `sasl.mechanism` | `SCRAM-SHA-512` | Auth mechanism |
| `ssl.truststore.location` | `/path/to/truststore.jks` | JVM trust store path |
| `ssl.keystore.location` | `/path/to/client.keystore.jks` | Client identity key |
| `sasl.jaas.config` | see below | JAAS login module |

Example JAAS config line:
```yaml
config: org.apache.kafka.common.security.scram.ScramLoginModule required username="producer" password="secret";
```

### 4. Producer/Consumer TLS Setup

Every service that connects to Kafka needs:

1. **Bootstrap servers** configured with `tls://` prefix
2. **SSL context** pointing to truststore and client keystore
3. **JAAS credentials** for SCRAM-SHA-512 authentication

In Java, this is wired via:
```java
@Bean
public ProducerFactory<String, String> producerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "tls://kafka-tls:9092");
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
    // ... additional TLS config
}
```

## Migration Guide

### From Plaintext to TLS

1. **Generate PKI**: `./scripts/kafka-pki-setup.sh`
2. **Copy secrets** to `./secrets/` in the compose project root
3. **Update Spring configs** to point at TLS endpoints
4. **Restart services** with TLS profile enabled

### From TLS Back to Plaintext

Simply run:
```bash
docker compose down
docker compose up -d   # No --profile flag → plaintext mode
```

## Troubleshooting

### Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| `SSLHandshakeException` | Client doesn't trust broker's CA | Verify truststore has root-ca.crt |
| `NotAuthorizedException` | Wrong username/password | Check JAAS config matches SCRAM user |
| `Broker not found` | advertised.listeners misconfigured | Ensure it resolves within container network |
| `Keystore password mismatch` | JKS passwords differ from env vars | Align `SSL_KEYSTORE_PASSWORD` with actual file |

### Debug Commands

```bash
# Test TLS connection from orchestrator container
docker exec -it agent-platform-orchestrator \
  sh -c 'openssl s_client -connect kafka-tls:9092 -servername kafka-tls'

# Validate broker certificate chain
docker exec agent-platform-kafka-tls \
  openssl verify -CAfile /etc/kafka/secrets/root-ca.crt \
  /etc/kafka/secrets/broker.crt

# List Kafka topics through SASL connection
KAFKA_OPTS="-Djava.security.auth.login.config=/etc/kafka/secrets/jaas.conf" \
  docker exec agent-platform-kafka-tls \
  /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Log Levels

Increase verbosity during migration:
```bash
# In docker-compose.yml per-service environments
logging.level.org.apache.kafka: DEBUG
logging.level.org.springframework.kafka: DEBUG
```

## Next Steps After SASL_SSL

- [ ] Enable mTLS end-to-end (both client→broker AND broker→broker)
- [ ] Add rate limiting at API gateway before Kafka
- [ ] Configure Prometheus alerting on authentication failures
- [ ] Integrate Vault dynamic credential rotation
- [ ] Set up log aggregation with OTel Collector (Jaeger traces visible in Grafana)
