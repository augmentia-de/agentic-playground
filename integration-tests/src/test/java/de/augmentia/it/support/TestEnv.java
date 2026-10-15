package de.augmentia.it.support;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Zugriff auf die beiden laufenden Plattformen und den Broker.
 *
 * <p>Absichtlich ohne Spring-Test-Framework: die Tests sollen gegen die <em>gepackten</em> Anwendungen
 * laufen, nicht gegen eine neu gebaute Testinstanz. Alles, was hier gebraucht wird, ist ein
 * HTTP-Client und ein Kafka-Client.
 */
public final class TestEnv {

    public static final String ORCHESTRATOR =
            setting("it.orchestrator.url", "ORCH_URL", "http://localhost:18080");
    public static final String QUAD =
            setting("it.quad.url", "QUAD_URL", "http://localhost:18086");
    public static final String BOOTSTRAP =
            setting("it.kafka.bootstrap", "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    public static final String WORKER =
            setting("it.worker.url", "WORKER_URL", "http://localhost:18081");

    /**
     * Logdatei des Orchestrators. Der Runner schreibt sie, {@code SicherheitSystemtest} liest
     * darin. Der Pfad folgt {@code RUN_DIR} (Default {@code /tmp/opencode/run/it}), damit er
     * zum Runner passt, ohne dass beide dieselbe Konstante wiederholen.
     */
    public static final java.nio.file.Path ORCHESTRATOR_LOG =
            java.nio.file.Path.of(setting("it.orchestrator.log", "ORCH_LOG",
                    setting("RUN_DIR", "RUN_DIR", "/tmp/opencode/run/it") + "/orch.log"));

    /**
     * Systemeigenschaft, sonst Umgebungsvariable, sonst Default. So laufen die Tests
     * sowohl ueber Maven ({@code -Dit.x=…}, landet als Systemeigenschaft) als auch
     * direkt (z.B. in der IDE) ueber Umgebungsvariablen.
     */
    private static String setting(String property, String env, String fallback) {
        var value = System.getProperty(property);
        if (value == null || value.isBlank()) {
            value = System.getenv(env);
        }
        return (value == null || value.isBlank()) ? fallback : value;
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();

    private TestEnv() {}

    // ── Verfügbarkeit ───────────────────────────────────────────────────────

    /*
     * Diese drei Hilfen duerfen nie werfen. Sie werden in @BeforeAll-Annotationen
     * aufgerufen, die ueber assumeTrue() einen Skip erzwingen sollen. Wirft get() bei
     * fehlendem Dienst, kommt statt des Skips ein Fehler — und der Build bricht ab,
     * obwohl die Tests sich korrekt als "nicht anwendbar" zurueckmelden wollten.
     */
    public static boolean orchestratorUp() {
        return reachable(ORCHESTRATOR + "/actuator/health");
    }

    public static boolean quadUp() {
        return reachable(QUAD + "/q/health");
    }

    public static boolean workerUp() {
        return reachable(WORKER + "/actuator/health");
    }

    /** true, wenn die Adresse erreichbar ist und mit 2xx antwortet. Wirft nie. */
    public static boolean reachable(String url) {
        try {
            return get(url).statusCode() == 200;
        } catch (RuntimeException e) {
            return false;
        }
    }

    public static boolean kafkaUp() {
        try (var admin = admin()) {
            admin.listTopics().names().get();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // ── HTTP ─────────────────────────────────────────────────────────────────

    public record Response(int statusCode, String body) {
        public JsonNode json() {
            try {
                return MAPPER.readTree(body);
            } catch (Exception e) {
                throw new IllegalStateException("Antwort ist kein JSON: " + body, e);
            }
        }

        public boolean ok() {
            return statusCode >= 200 && statusCode < 300;
        }
    }

    public static Response get(String pathOrUrl) {
        return send(request(pathOrUrl).GET().build());
    }

    public static Response postJson(String url, Object body) {
        return postJson(url, body, Map.of());
    }

    public static Response postJson(String url, Object body, Map<String, String> headers) {
        var builder = request(url).header("Content-Type", "application/json");
        headers.forEach(builder::header);
        return send(builder.POST(HttpRequest.BodyPublishers.ofString(toJson(body))).build());
    }

    public static Response putJson(String url, Object body) {
        return send(request(url)
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(toJson(body)))
                .build());
    }

    public static Response delete(String url) {
        return send(request(url).DELETE().build());
    }

    private static HttpRequest.Builder request(String pathOrUrl) {
        var target = pathOrUrl.startsWith("http") ? pathOrUrl : ORCHESTRATOR + pathOrUrl;
        return HttpRequest.newBuilder(URI.create(target)).timeout(Duration.ofSeconds(30));
    }

    private static Response send(HttpRequest req) {
        try {
            var res = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            return new Response(res.statusCode(), res.body());
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new IllegalStateException("HTTP fehlgeschlagen: " + req.uri(), e);
        }
    }

    public static String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Kann nicht serialisiert werden: " + value, e);
        }
    }

    // ── Kafka ────────────────────────────────────────────────────────────────

    public static Properties kafkaProps() {
        var p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        return p;
    }

    public static AdminClient admin() {
        var p = kafkaProps();
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        return AdminClient.create(p);
    }

    /**
     * Liest alle Nachrichten eines Topics ab dem aktuellen Ende.
     *
     * <p>{@code fromBeginning = true} liest ab Offset 0 und ignoriert das, was die Plattformen
     * untereinander bereits verarbeitet haben. Für Tests, die "was hat der Orchestrator
     * wirklich publiziert" prüfen wollen, ist das richtig; wer die Abwesenheit von Nachrichten
     * prüft, braucht stattdessen {@code readBefore}.
     */
    public static java.util.List<String> readTopic(String topic, int expected,
                                                  Duration timeout, boolean fromBeginning) {
        var out = new java.util.ArrayList<String>();
        var p = kafkaProps();
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "it-probe-" + java.util.UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, fromBeginning ? "earliest" : "latest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // eigene Gruppe => keine vorhandene Consumer-Group-Position stoert den Test
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        try (var consumer = new KafkaConsumer<String, String>(p, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(java.util.List.of(topic));
            var deadline = System.currentTimeMillis() + timeout.toMillis();
            while (out.size() < expected && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    out.add(r.value());
                }
            }
        }
        return out;
    }

    /** Wartet, bis der Topic mindestens {@code expected} Nachrichten hat, und liefert sie. */
    public static java.util.List<String> awaitTopic(String topic, int expected, Duration timeout) {
        return readTopic(topic, expected, timeout, true);
    }

    // ── Idempotente Hilfen ───────────────────────────────────────────────────

    /**
     * Schreibt eine einzelne Nachricht direkt auf ein Topic.
     *
     * <p>Dazu gibt es keinen Umweg über die Plattform: für Störfalltests muss eine Nachricht
     * erzeugt werden, die der Orchestrator unter normalen Umständen nie schreiben würde —
     * ein kaputtes JSON, ein Task ohne Header, ein Resultat zu einem Lauf, den es nicht gibt.
     *
     * <p>Das ist bewusst die einzige Stelle im Testcode, die Topics direkt beschreibt. Wer
     * darüber eine plausible Nachricht schickt, meint es gut; wer eine kaputte schickt,
     * will genau das prüfen. Der Kommentar hier ist die Grenze.
     */
    public static void publish(String topic, String key, String value) {
        var p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("key.serializer", StringSerializer.class);
        p.put("value.serializer", StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(p)) {
            producer.send(new ProducerRecord<>(topic, key, value)).get();
        } catch (Exception e) {
            throw new IllegalStateException("Publizieren auf " + topic + " fehlgeschlagen", e);
        }
    }

    /** Wie {@link #publish}, aber ohne auf die Zustellung zu warten. */
    public static void publishAsync(String topic, String key, String value) {
        var p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("key.serializer", StringSerializer.class);
        p.put("value.serializer", StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(p)) {
            producer.send(new ProducerRecord<>(topic, key, value));
        } catch (Exception e) {
            throw new IllegalStateException("Publizieren auf " + topic + " fehlgeschlagen", e);
        }
    }

    /** Schlüssel, der innerhalb eines JVM-Laufs einmal vergeben wird. */
    public static String uniqueKey(String prefix) {
        return prefix + "-" + java.util.UUID.randomUUID();
    }

    /** Liest ein Feld aus einem JSON-Objekt, sonst {@code null}. */
    public static Optional<String> field(JsonNode node, String name) {
        if (node == null || !node.hasNonNull(name)) return Optional.empty();
        return Optional.of(node.get(name).asText());
    }
}