# Integrationstests — kafka-platform × quad

Tests gegen die **fertigen Artefakte**: gepackte JARs, echter Kafka-Broker, echte Postgres,
echte HTTP-Aufrufe. Kein In-Process-Zugriff, keine Mock-Interfaces im Testpfad — nur der
Worker selbst arbeitet im Mock-Modus, weil ohne LLM-Key sonst nichts deterministisch liefe.

## Schnellstart

```bash
# alles: Stack hochfahren, 58 Tests, Stack wieder runter
./scripts/run-integration-tests.sh e2e

# nur L0+L1+L2 (Broker + Orchestrator genügen)
./scripts/run-integration-tests.sh

# Stack dauerhaft hochfahren, in einer anderen Shell testen
./scripts/run-integration-tests.sh --start
mvn -o test -Pe2e
./scripts/run-integration-tests.sh --stop
```

Der Runner setzt `--quarkus.base-url` der Bridge auf den quad-Port. Ohne das scheitert der
Proxy-Pfad der Unified-API mit 502 — siehe [Befund 3](#befund-3).

### Voraussetzungen

| Komponente | Erwartet | Variabel |
|---|---|---|
| Kafka | `localhost:9092` | `KAFKA_BOOTSTRAP_SERVERS` |
| Postgres | `jdbc:postgresql://localhost:5432/ap` | `POSTGRES_URL` |
| Orchestrator | Port `18080` | `ORCH_PORT` |
| Worker | Port `18081` | `WORKER_PORT` |
| quad | Port `18086` | `QUAD_PORT` |

Die Ports liegen bewusst neben den Standards (`8080`, `8086`, `8092`), damit ein laufender
Entwickler-Stack nicht mitgetroffen wird. Kafka und Postgres startet der Runner **nicht** — sie
sind die gemeinsame Infrastruktur, die auch der Entwickler-Stack nutzt.

Bevorzugt frisch: **eigener Broker**. Der Grund steht in [Befund 1](#befund-1) — die
Beobachtungen hängen davon ab, ob ein Topic vorher schon existierte.

## Aufbau

Vier Schichten. Jede baut auf der vorigen auf und prüft eine andere Fehlerklasse.

| Schicht | Test | Umgebung | prüft |
|---|---|---|---|
| **L0** | `l0contract/PlatformContractTest` | keine | Topic-Namen, Partitionierung, Task-/ResultEvent-JSON, DAG-Validator, Event-Roundtrip |
| **L1** | `l1api/OrchestratorApiTest` | Orchestrator | Health, Definitionen anlegen/ablehnen, Start, Idempotenz, 404, Checkpoints, HITL, Metrics |
| **L2** | `l2kafka/KafkaContractTest` | + Broker | Topics gegen Spezifikation, Intake-/Task-/Result-Events, Nachrichtenfluss |
| **L3** | `l3e2e/PlatformEndToEndTest` | + Worker | echte Läufe: Ende, HITL-Gate, Freigabe, Abbruch, Task- und Result-Events |
| **L3** | `l3e2e/CrossPlatformWorkflowTest` | + quad | quad-Definition → Bridge → kafka-Ausführung, beide Backend-Wege |

Fehlt eine Abhängigkeit, überspringt die Schicht per `assumeTrue` — der Build bleibt grün und
sagt, was fehlte. L0 läuft immer.

## Profile

```bash
mvn -o test              # L0 (Standard: alles außer Netzwerk)
mvn -o test -Pintegration   # L0 + L1 + L2
mvn -o test -Pe2e            # alles
```

Gesteuert über Tags: `requires-orchestrator`, `requires-kafka`, `requires-e2e`.

**Achtung beim Lesen der Zusammenfassung.** Läuft die Umgebung nicht, meldet Surefire
`Tests run: 18, Skipped: 0` — obwohl L1 und L2 durchaus beteiligt waren. Sie springen in
`@BeforeAll` über eine Annahme und zählen deshalb als **0 Tests**, nicht als „übersprungen“.
Im Report steht das als leere Klasse:

```
 18 tests   de.augmentia.it.l0contract.PlatformContractTest
  0 tests   de.augmentia.it.l1api.OrchestratorApiTest      ← per Annahme übersprungen
  0 tests   de.augmentia.it.l2kafka.KafkaContractTest      ← per Annahme übersprungen
```

Wer „18 grün“ sieht, sollte also gegenprüfen, ob die Umgebung stand. Der Runner startet sie
deshalb selbst, statt sich darauf zu verlassen.

## Aktueller Stand

```
L0  PlatformContractTest        18 Tests   grün
L1  OrchestratorApiTest         14 Tests   grün
L2  KafkaContractTest           11 Tests   grün, 1 @Disabled (Befund 1)
L3  PlatformEndToEndTest         8 Tests   grün
L3  CrossPlatformWorkflowTest    7 Tests   grün, 1 @Disabled (Befund 2)
──────────────────────────────────────────────────────────
    58 Tests, 0 Fehler, 2 dokumentierte Befunde
```

---

# Befunde

Zwei echte Fehler im Produktivcode. Beide sind als `@Disabled`-Test formuliert — die Assertion
ist die richtige Erwartung und wird aktiv, sobald der Fehler behoben ist. Dazu steht jeweils ein
grüner Test, der den heutigen Zustand festhält: der Befund ist damit nicht nur notiert,
sondern messbar. Sobald sich das Verhalten ändert, fällt **zuerst** der grüne Ist-Test um und
zeigt die Änderung.

## Befund 1 — wer zuerst publiziert, legt das Topic fest

`KafkaTopicInitializer` ist **create-if-missing**:

```java
Set<String> existing = adminClient.listTopics().names().get();
List<NewTopic> missing = specs.stream()
        .filter(spec -> !existing.contains(spec.name()))   // ← vorhandene werden übergangen
        .map(KafkaTopicInitializer::toNewTopic)
        .toList();
```

Ein Topic, das schon existiert, behält seine Broker-Vorgaben — ohne Hinweis, ohne Log.
Beobachtet auf einem Broker, der schon vorher benutzt wurde:

| Topic | Spec | Broker |
|---|---|---|
| `agent.tasks.*` (10 Topics) | 12 | **12** |
| `agent.control.timeouts` | 6 | **6** |
| `agent.execution.dlq` | 6 | **6** |
| `agent.control.sweep` | 1 | 1 |
| `agent.execution.intake` | 12 | **1** |
| `agent.execution.results` | 12 | **1** |
| `agent.control.cancellation` | 6 | **1** |
| `agent.control.hitl` | 6 | **1** |
| `agent.control.loop` | 6 | **1** |
| `agent.config.updates` | compact | **delete** |

Das Muster ist eindeutig: `agent.tasks.*`, `timeouts` und `dlq` waren beim ersten Start noch
nicht da und bekamen die Spezifikation. Alles andere hatte jemand schon vorher berührt — der
Broker hat es mit einer Partition und `cleanup.policy=delete` angelegt, und der Initializer
hat das kommentarlos akzeptiert.

**Warum das mehr als Kosmetik ist.** Zwei Folgen, die unterschiedlich schwer wiegen:

- **Parallelität.** `INTAKE` und `RESULTS` sollen 12 Partitionen haben. Mit einer Partition
  läuft die gesamte Plattform serialisiert. Das fällt nicht auf, weil die Tests weiterlaufen —
  es wird nur langsamer.
- **Kompaktierung.** `config.updates` ist als `compact` definiert, damit nur die jeweils letzte
  Konfiguration pro `agent_type` erhalten bleibt. Mit `delete` wächst das Topic stattdessen, bis
  die Retention greift. Die Fälle sind nicht gleich, aber die Datenmenge ist begrenzt.

Der Test prüft das topicweise gegen `KafkaTopics.all()` — nicht topicweise handgepflückt, weil
die Liste sich mit jedem Lauf ändert. Deshalb nennt die Fehlermeldung immer den vollständigen
Satz Abweichungen.

**Fix-Richtung:** Der Initializer sollte abgleichen statt nur anlegen — `createPartitions` für
zu wenige Partitionen, `alterConfigs` für `cleanup.policy` und `retention.ms`. Beides sind
idempotente Admin-Operationen.

Tests: `brokerMatchesTopicSpecification` (`@Disabled`), `driftIsLimitedToTopicsCreatedByTheBroker`,
`workerTopicsAreFullyPartitioned`, `configUpdatesCompactionComesFromTheTopicCreator`.

## Befund 2 — quad meldet einen leeren Workflow als `completed`

`POST /api/ui/workflows` mit `nodes: []` wird angenommen. Der Workflow lässt sich starten, und
der Lauf endet mit Status `completed`.

Damit bedeutet `completed` nichts — es lief schlicht nichts. Ein Workflow ohne Knoten sollte
beim Anlegen oder spätestens beim Start abgelehnt werden. Wer auf `completed` prüft, um
Erfolg zu signalisieren, bekommt hier ein falsches Ja.

Tests: `quadRejectsEmptyWorkflow` (`@Disabled`), `emptyWorkflowRunsAndReportsCompleted`.

---

# Konfigurationsfallen

Keine Fehler, aber beim Einrichten aufgetroffen — hier, damit sie niemand neu suchen muss.

**`quarkus.base-url` der Bridge.** Der Orchestrator läuft mit
`quarkus.base-url=http://localhost:8086`. Quad läuft standardmäßig auf `8086`, im Test auf
`18086`. Ohne Anpassung liefert `POST /api/v1/unified/workflows/from-quad` mit
`X-Backend: quarkus` ein `502` — die Fehlermeldung nennt immerhin die unerreichbare Adresse.
Der Runner setzt das Argument, deshalb ist der Test grün. Für einen echten Betrieb gehört
`quarkus.base-url` in die Konfiguration statt auf den Default zu zeigen.

**Worker-Port.** In `quad-workflow-worker/src/main/resources/application.yml` steht
`server.port: 8092` fest, ohne `${...:-Default}` — also **nicht** per Environment
überschreibbar. Der Runner setzt `--server.port`. Wenn der Worker wirklich mal auf einem
anderen Port laufen soll, ist das eine Config-Zeile, keine Systemeigenschaft.

**quad-Datenverzeichnis.** quad legt seine SQLite-Datei als `data/quad.db` an, aufgelöst gegen
das **Arbeitsverzeichnis** des Prozesses, nicht gegen das Jar. Der Runner startet quad deshalb
in `$RUN_DIR/quad`. Aus dem Modulverzeichnis gestartet, findet Flyway die Datei nicht und
quad startet gar nicht.

**Der Legacy-Worker hört nicht mehr auf `agent.tasks.*`.** `agent-worker` ist auf
`legacy\.agent\.tasks\..*` umgestellt, weil der kanonische Worker (`quad-workflow-worker`)
diese Topics übernommen hat. Im Test startet der Runner deshalb
`quad-workflow-worker` — der `agent-worker` wäre still und die L3-Tests würden hängen.

## Notiert, nicht als Test festgeschrieben

- **`TaskEvent.header` ist nicht validierungspflichtig.** Das Feld darf `null` sein
  (`TaskEvent.java`). Für Korrelation und Timeout-Überwachung ist der Header aber der einzige
  Weg, einen Task einem Lauf zuzuordnen. L0 hält den heutigen Vertrag fest; die Frage, ob das
  gewollt ist, gehört in die Produktentscheidung.
- **Der HITL-Freigabepfad braucht Zeit.** Nach dem Retry eines pausierten Laufs laufen die
  restlichen Schritte asynchron weiter. Der Test wartet auf `COMPLETED` und darf `PAUSED_HITL`
  dort **nicht** in der Warteliste haben — sonst kehrt er sofort zurück, bevor die Freigabe
  verarbeitet ist.