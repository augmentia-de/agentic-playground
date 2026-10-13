package de.augmentia.agentplatform.orchestrator.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import javax.net.ssl.TrustManagerFactory;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;

/**
 * TLS/mTLS + SCRAM-SHA-512 Kafka client configuration for production environments.
 * Activated via profile 'tls' — sets up SSL transport layer with mutual authentication
 * and SCRAM-SHA-512 SASL mechanism for both producers and consumers.
 *
 * <h3>Security Layers (bottom → top)</h3>
 * <pre>
 * TCP ──► TLS/mTLS (certificate-based encryption)
 *        ──► SASL/SCRAM-SHA-512 (username/password auth)
 *        ──► Application Layer (workflow orchestration)
 * </pre>
 */
@Configuration
@ConditionalOnProperty(name = "spring.profiles.active", havingValue = "tls")
public class KafkaTlsSecurityConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaTlsSecurityConfig.class);

    @Value("${spring.kafka.bootstrap-servers:kafka-tls:9092}")
    private String bootstrapServers;

    @Value("${security.protocol:SASL_SSL}")
    private String securityProtocol;

    @Value("${sasl.mechanism:SCRAM-SHA-512}")
    private String saslMechanism;

    @Value("${sasl.jaas.config:")
    private String jaasConfig;

    @Value("${ssl.truststore.location:/etc/kafka/secrets/truststore.jks}")
    private String truststoreLocation;

    @Value("${ssl.truststore.password:kfk-pass}")
    private String truststorePassword;

    @Value("${ssl.keystore.location:/etc/kafka/secrets/orchestrator-keystore.jks}")
    private String keystoreLocation;

    @Value("${ssl.keystore.password:kfk-pass}")
    private String keystorePassword;

    @Value("${ssl.endpoint.identification.algorithm:https}")
    private String endpointIdAlgorithm;

    /** Configure Producer factory with TLS/mTLS + SCRAM */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        
        // Bootstrap servers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        // Security
        configureSecurity(props);
        
        // Serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        
        // Reliability
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        log.info("Kafka producer configured with TLS + SASL on {}", bootstrapServers);
        return new DefaultKafkaProducerFactory<>(props);
    }

    /** Configure Consumer factory with TLS/mTLS + SCRAM */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        
        // Bootstrap servers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        // Security
        configureSecurity(props);
        
        // Deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);
        
        // Auto offset & isolation
        String isoKey = "isolation.level";  // string key per Kafka docs
        props.put(isoKey, "read_committed");
        
        // Manual ack (no auto-commit)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        log.info("Kafka consumer configured with TLS + SASL on {}", bootstrapServers);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /** Configure listener container factory with TLS security */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(
                org.springframework.kafka.listener.ContainerProperties.AckMode.RECORD);
        factory.setConcurrency(1);
        return factory;
    }

    /** Apply TLS/SASL configuration to all Kafka client configs */
    private void configureSecurity(Map<String, Object> props) {
        // Security protocol + SCRAM-SHA-512 SASL
        String secProto = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
        props.put(secProto, securityProtocol);
        props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
        
        // SSL keystore/truststore — use string config keys (not non-existent constants)
        String trustLoc = "ssl.truststore.location";
        String trustPass = "ssl.truststore.password";
        String ksLoc = "ssl.keystore.location";
        String ksPass = "ssl.keystore.password";
        String epIdAlg = "ssl.endpoint.identification.algorithm";
        
        props.put(trustLoc, truststoreLocation);
        props.put(trustPass, truststorePassword);
        props.put(ksLoc, keystoreLocation);
        props.put(ksPass, keystorePassword);
        props.put(epIdAlg, endpointIdAlgorithm);
    }

    @jakarta.annotation.PostConstruct
    public void validateTruststore() {
        try {
            KeyStore ks = KeyStore.getInstance("JKS");
            try (var is = new java.io.FileInputStream(truststoreLocation)) {
                ks.load(is, truststorePassword.toCharArray());
            }
            log.info("✅ Truststore loaded from {} with {} entries", truststoreLocation, ks.size());
            
            // Validate broker certificate chain
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            log.info("✅ TrustManager initialized successfully");
        } catch (Exception e) {
            log.error("❌ Failed to load truststore: {}. Ensure PKI files exist.", e.getMessage());
            throw new IllegalStateException("TLS configuration error — check certificates");
        }
    }
}
