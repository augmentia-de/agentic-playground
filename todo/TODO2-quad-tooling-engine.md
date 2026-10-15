# TODO2 — quad tooling layer & workflow engine: consolidated change list

> **Status:** tracked in git (WP14). Originally an untracked `TODO2.md` in the repo root;
> this is now the versioned backlog for the quad tool layer and workflow engine.
> The **P0 block (items A1–A12 / B1–B8, "Umsetzungsstand: P0-Block ist durch") is
> implemented and green** — see the summary table below. The P1/P2 items are a backlog,
> not a promise: cross-check each against the current code and the `todo/WP*.md` work
> packages before starting it.
>
> Provenance: consolidated from the (no longer present) `strands2quad.md` and
> `myclaw2quad-reviewed.md` analyses.
>
> **WP13 note:** the dead classes `ToolSetChangeNotifier` and `StateDiffMemoryBridge`
> mentioned in item A11 were removed as unreachable; `notifyToolSetChanged` no longer
> exists as a live extension point.

---

# TODO2 — Tool-Schicht und Workflow-Engine: konsolidierte Änderungsliste

Zusammenführung der beiden Analysen zu **einer** umsetzbaren Liste, mit geprüften Auswirkungen auf
`kafka-platform`.

| Quelle | Thema | Umfang |
|---|---|---|
| `strands2quad.md` | Tool-Schicht in `quad-core` | `ReflectiveToolMethod`, `QuadToolRegistry`, `ToolExecutor`, Annotationen |
| `myclaw2quad-reviewed.md` | Workflow-Engine, Prompt-Rendering | `LoopNodeExecutor`, `InputBuilder`, `PromptAssembler`, `WorkflowExecutionContext` |

## Umsetzungsstand: P0-Block ist durch

Der P0-Block (Maßnahmen 1–12) ist implementiert und getestet. Beide Builds sind grün, ohne
übersprungene Tests in den geänderten Klassen.

| | vorher | nachher |
|---|---|---|
| quad gesamt | 672 Tests, 0 Fehler, 22 `@Disabled` | **692 Tests, 0 Fehler, 0 `@Disabled`** |
| quad-core | 436 Tests | 441 |
| quad-workflow-engine | 45 Tests | 56 |
| kafka-platform | 278 Tests, 5 key-gated | 278 Tests, 5 key-gated |

**Geänderte Dateien (17 Produktivdateien, ~990 Zeilen):**

| Datei | Änderung |
|---|---|
| `quad-quarkus/pom.xml` | `<parameters>true</parameters>` (A1) |
| `quad-core/.../tool/ReflectiveToolMethod.java` | neu: Schema immer, `convertValue`, `try/finally`, State aus Argument |
| `quad-core/.../tool/ToolMeta.java` | **neu** |
| `quad-core/.../tool/ToolMethod.java` | `meta()` als Default-Methode |
| `quad-core/.../tool/QuadToolRegistry.java` | `find`, `size`, `getToolNames`, `contains`, harte Typen, `DuplicateToolNameException` |
| `quad-core/.../tool/ToolActivatorTool.java` | Fehlerpfade auf `ToolResult.error`, Parameterschema |
| `quad-core/.../tool/GizmoInvoker.java` | **entfernt** (unreferenziert) |
| `quad-core/.../agent/runtime/ToolExecutor.java` | `isIdempotentTool` liest `ToolMeta` |
| `quad-core/.../agent/runtime/DynamicToolProvider.java` | `createToolInvoker` wirft statt zu erfinden |
| `quad-core/.../agent/PromptAssembler.java` | Nicht-JSON-Prompt passiert unberührt (B1) |
| `quad-workflow-engine/.../internal/WorkflowExecutionContext.java` | `outputHistory`, `OutputRecord`, `resolveHistory`, `clearOutput`, Snapshot v3 |
| `quad-workflow-engine/.../node/executor/LoopNodeExecutor.java` | `exitConditionSource`, `aggregate`, `retainOutput`, Historie-erhaltendes Leeren |
| `quad-workflow-engine/.../node/executor/AgentNodeExecutor.java` | Template-Pfad von `isJsonOutputMode` entkoppelt |
| `quad-workflow-engine/.../context/InputBuilder.java` | `inputMappings` mit `scope`, `renderTemplateStrict`, `previousOutput` korrigiert |

**Geprüfte Stände**

- quad: `/home/torsten/dev/my-projects/agentic-platform/quad-quarkus`, Branch `master`
- kafka-platform: `/home/torsten/dev/my-projects/agentic-platform/kafka-platform`, Branch `main`, HEAD `7b5b9cb`
- myclaw: `/home/torsten/dev/my-projects/myclaw`, Branch `master`, HEAD `390e560` (nur Referenz)
- strands: `/home/torsten/dev/my-projects/strands-agents-java-1/strands-agents` (nur Referenz)

**Reihenfolge der Umsetzung:** B1 zuerst, weil jede Template-Arbeit sonst gegen eine Strecke gebaut
worden wäre, die ihr Ergebnis verwirft. Dann A1–A7 als Tool-Schicht-Block, dann B2–B8 als
Engine-Block, dann B5–B7. Der Reihenfolge folgend wurden die Tests nach jedem Schritt von
`@Disabled` auf grün umgestellt.

---

## 0. Kurzfassung für die Reihenfolge

Drei Dinge blockieren alles Weitere:

1. **`-parameters` fehlt in quad** (A1). Ohne das ist jeder namensbasierte Fix eine Zeitbombe.
2. **Doppeltes Template-Rendering** (B1). Der zweite Durchgang verwirft den ersten — aufgelöste
   Werte erreichen das Modell nicht. Jede Arbeit an Templates wäre gegen diese Strecke gebaut.
3. **`parameters = null` bei rein optionalen Tools** (A2). Trifft real `LsTool`, und `LsTool` ist im
   kafka-platform-Worker produktiv registriert.

Danach folgen die Bindungs-, Registry- und Datenfluss-Korrekturen. Erst ganz am Ende der Umbau auf
`ToolMeta`/`ToolInvocation`/`ToolCatalog`, weil er sonst auf einer noch defekten Datenebene aufsetzt.

**kafka-platform ist nur von Block 1 direkt betroffen** und dort bereits vorkonfiguriert. Details in
[Abschnitt D](#d-auswirkungen-auf-kafka-platform).

---

## A — Tool-Schicht (`quad-core`)

### A1. `-parameters` im quad-Build setzen — **Voraussetzung**

Kein POM im quad-Reaktor setzt den Compiler-Arg. Bytecode-Nachweis:

| Modul | Klassen | mit echten Parameternamen |
|---|---|---|
| quad-core | 286 | **0** |
| quad-tool-builtin | 11 | **0** |
| quad-tool-mcp | 5 | **0** |
| quad-sandbox-docker | 5 | **0** |
| quad-workflow-core | 36 | 0 |
| strands-agents | 100 | 96 |

Die 93 `MethodParameters`-Treffer in quad-core sind ausschließlich Records und Enums — vom Compiler
per Sprachspezifikation verpflichtet, nicht durch `-parameters`. `ReflectiveToolMethod.class` hat
null, strands' `FindTool.class` hat sechs.

**Änderung:** `quad-quarkus/pom.xml:149–156`, `maven-compiler-plugin` um `<parameters>true</parameters>`
ergänzen. Alle zehn Module erben es über den Parent.

**Warum zuerst:** `ReflectiveToolMethod:53` nutzt `param.getName()` als Fallback, wenn `@Param`
fehlt. Ohne `-parameters` liefert das `arg0`, `arg1`. Der LLM sendet `path`, das Binding sucht
`arg0`, der Parameter bleibt `null`. Empirisch:

```
Spec-Parameter : {arg0=JsonStringSchema, required=[arg0]}
Aufruf mit {"path":"a.txt"} -> "content of null"      <-- still falsch, keine Exception
Aufruf mit {"arg0":"a.txt"} -> "content of a.txt"
```

Alle neun quad-Builtins umgehen das heute durch konsequentes `@Param`. Jedes neue Tool ohne `@Param`
fällt in das Loch.

**Test:** ein Tool ohne `@Param` registrieren und die generierten Parameternamen prüfen
(`path`, nicht `arg0`).

---

### A2. `buildSpec`: `.parameters()` immer setzen — **P0, real betroffen**

`ReflectiveToolMethod:65–68` ruft `.parameters()` nur bei nicht-leerer `required`-Liste:

```java
if (!required.isEmpty()) {
    schemaBuilder.required(required.toArray(new String[0]));
    specBuilder.parameters(schemaBuilder.build());   // nur dann
}
```

Ein Tool, dessen Parameter **alle** `required = false` sind, erhält `parameters = null` — das Modell
sieht kein Parameter-Schema. Betroffen ist real `LsTool` (`:46–50`, vier optionale Parameter).

```
alle Params optional -> parameters = null
@Tool ohne Text      -> description = []
```

**Änderung:** Bedingung entkoppeln, `required` nur setzen wenn nicht leer.

**kafka-platform-Impact:** `LsTool` ist in `QuadWorkerConfig:123–126` als Bean registriert und geht
über `ToolCatalog` in den Katalog. Der Defekt ist damit produktiv im Worker, wird aber nur wirksam,
wenn das Modell `listDirectory` aufruft — bisher nutzt der Mock-Pfad es nicht. **Auswirkung auf den
Worker-Betrieb: keine Verhaltensänderung bei gleichem Modellverhalten, aber die Lücke ist real.**

---

### A3. Argument-Binding auf `convertValue` umstellen — **P0**

`ReflectiveToolMethod.mapValue:173–183` deckt fünf Typen ab, alles andere fällt auf `toString()`.
Empirisch gegen den echten Build:

| Parameter | JSON | Ergebnis |
|---|---|---|
| `List<EditOperation>` | `{"edits":[{...}]}` | `IllegalArgumentException: argument type mismatch` |
| `Map<String,Object>` | `{...}` | `IllegalArgumentException: argument type mismatch` |
| `String` | `{"x":1}` | `""` — still, leer |
| `String` | `["a","b"]` | `""` — still, leer |
| `int` | `{}` | `IllegalArgumentException` (NPE in `ValueConversions`) |

**Wichtig für die Priorisierung:** der `toString()`-Fallback erzeugt bei `List`/`Map`/`Record`/`POJO`
keine stille Korruption, sondern einen harten Fehler. `ToolExecutor:144` fängt ihn und gibt
`ToolResult.error("Tool Execution Error [multiEdit]: ...")` ans Modell. Betroffen ist real
`MultiEditTool:37–38` (`List<EditOperation>`), das **laut kaputt** ist. Der stille Pfad ist
`String`-Parameter mit Objekt-/Array-Argument.

**Änderung:** `buildArgs` auf `mapper.convertValue(v, paramType)` umstellen, `AgentSessionState`
aus dem übergebenen `state` statt aus dem ThreadLocal befüllen (siehe A7), fehlende Pflichtargumente
bei Primitiven mit sprechender Meldung ablehnen.

**Nicht umsetzen:** `ToolSpecifications.toolSpecificationFrom` als Ersatz für `buildSpec`. Es liest
`dev.langchain4j.agent.tool.Tool` und `@P`; quad hat eigene Annotationen. Getestet:

```
FAIL: NullPointerException: Cannot invoke "Tool.name()" because "tool" is null
```

`rg "@P("` über quad-core, quad-tool-builtin, quad-tool-mcp, quad-quarkus: null Treffer. Eine reine
Delegation würde alle neun Builtins zerlegen.

**kafka-platform-Impact:** die sieben im Worker registrierten Tools (`QuadWorkerConfig:108–140`) haben
ausschließlich `String`/`Integer`/`Boolean`-Parameter — sie sind von A3 **nicht** betroffen. `ToolCatalog:31`
ruft den `ReflectiveToolMethod`-Konstruktor direkt auf, das bleibt unverändert gültig.

---

### A4. `CurrentSession.clear()` + `try/finally` um `buildArgs` — **P0**

`ReflectiveToolMethod:121–123`:

```java
CurrentSession.setCurrent(state);
Object[] args = buildArgs(jsonArguments);   // liegt AUSSERHALB des try
try { … }
```

`CurrentSession` hat `bind`/`setCurrent`, aber **kein `clear()`**. Verschärfend: `buildArgs` liegt
außerhalb des `try`, jeder Argument-Parse-Fehler leakt ebenfalls.

**Änderung:** `CurrentSession.clear()` ergänzen, `execute` in `try/finally` um `setCurrent` **und**
`buildArgs` herumziehen.

**kafka-platform-Impact:** `TaskExecutor:216–221` bindet die Session bereits selbst und räumt in
`finally` mit `setCurrent(null)` auf. Der äußere Rahmen ist also sauber; das von A4 geschlossene Fenster
liegt **innerhalb** eines Tool-Aufrufs und betrifft den Worker nicht, solange der Task-Rahmen hält. Kein
Handlungsbedarf, aber die Begründung im Kommentar `TaskExecutor:37–41` („`CurrentSession` ist ein reines
ThreadLocal — der ReAct-Loop und die Tool-Injektion lesen es direkt") beschreibt exakt die Kopplung,
die A7 auflösen würde.

---

### A5. `ToolActivatorTool`: Fehler → `ToolResult.error` — **P0**

`ToolActivatorTool:45, 51, 59` verpacken alle Fehlerfälle als Erfolg:

```java
45: return ToolResult.success("Both 'action' and 'tool' parameters are required.");
51: yield   ToolResult.success("Unknown tool '" + tool + "'...");
59: default -> ToolResult.success("Unknown action '" + action + "'...");
```

Das Modell kann Fehler nicht von Bestätigungen unterscheiden und wiederholt den Aufruf. strands macht
es richtig (`ToolActivator.java:77, 82, 89`). Zusätzlich fehlt dem Spec jedes Parameter-Schema
(`:23–28`) — das Modell soll `action`/`tool` senden, bekommt aber keine Definition.

**Änderung:** Fehlerpfade auf `ToolResult.error`, Spec aus deklarativer Beschreibung erzeugen.

**kafka-platform-Impact: keiner.** `rg "ToolActivator"` im Worker: null Treffer.

---

### A6. `isIdempotentTool` auf Tool-Metadaten umstellen — **P0**

`ToolExecutor:200–207` prüft, ob die **`spec()`-Methode der `ToolMethod`-Klasse** mit `@Idempotent`
annotiert ist — nicht die annotierte Tool-Methode. Für `ReflectiveToolMethod` und `GizmoInvoker`
immer `false`, weil deren `spec()` die Annotation nie trägt.

Präzisierung: „immer `false`" gilt nur für diese beiden Implementierungen. Sobald `@Idempotent` auf
`spec()` sitzt, greift der Cache — genau so testet es `ToolExecutorTest:23`. Der Defekt ist ein Test,
der den falschen Ort zementiert und den produktiven Pfad nie ausführt.

**Änderung:** Metadaten in die Tool-Definition materialisieren (siehe A9), `ToolExecutor` darauf
umstellen, `ToolExecutorTest` auf eine echte Tool-Methode umschreiben.

**kafka-platform-Impact: keiner direkt.** `rg "Idempotent"` im Worker: nur ein Kommentar in
`agent-worker/.../PrefixCache.java` („only sets if not already present"), keine Annotation, keine
Nutzung von `IdempotencyStore.computeKey` über den Tool-Pfad. `WorkerAgentFactory:138` verdrahtet den
Store, `ToolExecutor` ruft ihn intern. Verhalten ändert sich nur, wenn jemand `@Idempotent` setzt.

---

### A7. `state` als Parameter statt ThreadLocal — **P1, Grundlage für A8**

`ReflectiveToolMethod:142–144` befüllt den `AgentSessionState`-Parameter aus dem ThreadLocal statt aus
dem `state`, den `execute` bereits entgegengenommen hat. Der ThreadLocal-Wert gewinnt — ein
eigenständiger Inkonsistenz-Weg, der in `strands2quad.md` nicht benannt war.

**Änderung:** `state` durchreichen, `CurrentSession` nur noch als Kompatibilitätsweg für
Tool-Code, der den State per `currentState()` zieht (`ReadFileTool:29–31`).

**kafka-platform-Impact: relevant.** `ReadFileTool`, `WriteTool`, `GrepTool`, `LsTool`, `FindTool`,
`WebSearchTool`, `WebFetchTool` rufen alle `currentState()` → `currentSession.get()` →
`CurrentSession.getCurrent()`. Ein Wegfall des ThreadLocal erfordert, dass diese sieben Klassen
umgestellt werden — oder dass der ThreadLocal als Fallback bleibt. **Empfehlung: ThreadLocal behalten,
`state` als bevorzugte Quelle ergänzen.** Sonst bricht der Worker.

---

### A8. `ToolInvocation`-Context einführen — **P1**

`ToolMethod.execute(String jsonArguments, AgentSessionState state)` hat kein `toolCallId`, kein
`AtomicBoolean abort`, kein `onUpdate`. `ToolExecutor:74` prüft Abbruch genau einmal
(`Thread.currentThread().isInterrupted()`), ein Timeout fehlt vollständig
(`rg "timeout"` in `ToolExecutor.java`: null Treffer). strands hat `abortFlag` plus Timeout-Cancel.

**Änderung:** `record ToolInvocation(toolCallId, agentId, sessionId, jsonArguments, state, aborted,
onProgress)`, `ToolMethod.execute(ToolInvocation)`.

**kafka-platform-Impact: hoch, aber beherrschbar.** `ToolMethod` ist ein **quad-core-Interface**, und
der Worker hält es in drei Signaturen:

| Stelle | Verwendung |
|---|---|
| `ToolCatalog:19, 27` | `Map<String, ToolMethod>`, Konstruktion via `ReflectiveToolMethod` |
| `ToolCatalog:48, 59` | `require(String)` / `tryGet(String)` liefern `ToolMethod` |
| `AgentToolRegistryFactory:6, 34` | `import`, `registry.register(name, …)` |

Der Worker **implementiert** `ToolMethod` nicht selbst — er erzeugt nur Instanzen über
`ReflectiveToolMethod`. Eine Signaturänderung an `execute` ist damit binär und quellspezifisch
**unkritisch**, solange `ReflectiveToolMethod` die neue Signatur bedient. `ToolCatalogTest` prüft nur
Namen und `spec()`, nicht `execute`. **Kein Anpassungsbedarf im Worker.**

---

### A9. `ToolMeta` materialisieren, tote Annotationen verdrahten — **P1**

| Deklariert | Gelesen? |
|---|---|
| `@Tool`, `@Param` | ja (`ReflectiveToolMethod:50, 84`) |
| `@Hidden` | **nein**, nirgends angewandt |
| `@GenerationMethod` | **nein**, nirgends angewandt |
| `@ToolRole` | **nein**, nirgends angewandt |
| `@SagaAgent` | **nein**, nirgends angewandt (`@InterceptorBinding`, nie aktiviert) |

`ReflectiveToolMethod` ist 184 Zeilen, davon der größte Teil Meta-Reflexion. Mit einem
`ToolMeta`-Record bei der Registrierung bliebe Ausführung bei ~40 Zeilen.

**kafka-platform-Impact: einer.** `ToolCatalog:27` prüft `method.isAnnotationPresent(Tool.class)` —
mit einer `ToolMeta`-Materialisierung in `ReflectiveToolMethod` bliebe diese Prüfung gültig, aber sie
liefe vor der Konstruktion und müsste sich nicht mehr auf die Annotation verlassen. Geringer Aufwand,
sauber zu lösen.

---

### A10. Registry-Vertrag härten — **P1**

| Aspekt | strands | quad |
|---|---|---|
| `get(name)` bei unbekannt | wirft `IllegalArgumentException` | gibt `null` zurück (`:80–82`) |
| `getSpecifications()` | `List<ToolSpecification>` | `List<?>` → **vier** Casts |
| `size()`, `removeAll()`, `getToolNames()` | vorhanden | fehlen alle drei |
| `withOnly(names)` | Kopie | Kopie (`:92–99`) |
| Kollision | still (last-wins) | Konstruktor loggt (`:38`), `registerBuiltIn` still (`:65–69`) |

Die vier Casts: `AgentRuntime:215`, `Agent:387`, `PromptAssembler:68`, `DynamicToolProvider:25`.

**Änderung:** `Optional<Tool> find(String)`, `List<ToolSpecification> getSpecifications()`,
`DuplicateToolNameException` mit Angabe des gewinnenden Providers, `size()`/`getToolNames()` ergänzen.

**kafka-platform-Impact: einer, unkritisch.** `AgentToolRegistryFactory:28–37` erzeugt pro Task eine
`QuadToolRegistry` und ruft `registerFromAgent` sowie `register`. Beides bleibt gültig. `ToolCatalog`
hat mit `require()`/`tryGet()` bereits fail-fast- bzw. null-Verhalten und ist von der
`get`-Änderung nicht betroffen, weil es seine eigene Map hält.

---

### A11. `GizmoInvoker`, `createToolInvoker`, `notifyToolSetChanged` — **P1**

```java
// DynamicToolProvider:38–43 — gibt die Argumente zurück, nicht das Ergebnis
return jsonArgs -> { return jsonArgs; };
// QuadToolRegistry:76–78 — No-op
public void notifyToolSetChanged(Set<String> added, Set<String> removed) { }
```

`GizmoInvoker` ist **vollständig unreferenziert** — `rg "GizmoInvoker"` findet nur die eigene Datei.

**Änderung:** entfernen oder `@Deprecated(forRemoval)` mit `throw new UnsupportedOperationException`.
Optional `ToolSetChangedEvent` (existiert bereits als Record, wird nie gefeuert) an `AgentLoop`
anschließen und eine `SYSTEM NOTE` injizieren — strands hat das Vorbild in
`AgentLoop.handleBeforeModelCall:225–246`.

**kafka-platform-Impact: keiner.** Alle drei Symbole kommen im Worker nicht vor.

---

### A12. Tool-Beschreibungen validieren, `StuckDetector` übernehmen — **P2**

strands validiert bei der Registrierung (`ToolRegistry.validate:59–69`, `strictValidation`): Beschreibung
vorhanden, ≥10 Zeichen, ≤2000 Zeichen. `ReflectiveToolMethod.buildDescription:83–90` liefert `""` bei
`@Tool` ohne Text — das Modell bekommt namenlose Werkzeuge. `StuckDetector` fehlt in quad ebenfalls.

**Änderung:** `ToolDescriptionValidator` übernehmen, aber beim Agent-Start validieren statt beim
Registrieren (beim Registrieren gibt es noch kein LLM, das man warnen könnte). `StuckDetector` für
wiederholte identische Tool-Calls.

**kafka-platform-Impact: keiner direkt, indirekt über die Tool-Auswahl in `QuadWorkerConfig:144–158`.**
Ein Validierungsfehler beim Start wäre dort ein Bean-Startup-Problem. Deshalb: als **Warnung** loggen,
nicht werfen.

---

## B — Workflow-Engine und Prompt-Rendering

### B1. Doppeltes Template-Rendering auflösen — **P0, blockiert alles Weitere**

Der schwerwiegendste Fund beider Analysen. `InputBuilder.renderTemplate` ist **nicht** die einzige
Strecke.

**Kette, im Quelltext verifiziert:**

1. `AgentNodeExecutor:80–82` — wenn `userMessageTemplate` **und** `isJsonOutputMode(config)`:
   `prompt2 = inputBuilder.renderTemplate(umt, templateVars)`. **Erstes** Rendering.
2. `AgentNodeExecutor:248–250` (in `configureAgent`, aufgerufen `:76`) — setzt zusätzlich
   `agent.setUserMessageTemplate(umt)`. Das Template wird **ein zweites Mal** hinterlegt.
3. `AgentNodeExecutor:251–253` — `agent.setJsonInput(true)`, sobald `isJsonOutputMode` gilt.
4. `AgentRuntime:107` → `Agent.initialMessages(prompt2, state)` (`Agent:379–381`) →
   `PromptAssembler.buildInitialMessages(prompt2, state, systemPrompt, /*jsonInput=*/true, umt, …)`.
5. `PromptAssembler:122–127` — `renderUserMessageTemplate(prompt, userMessageTemplate)`.
   **Zweites** Rendering.

**Was passiert:** `PromptAssembler.renderUserMessageTemplate:170–189` behandelt `inputJson` als
JSON-Dokument. `prompt2` ist **kein** JSON, sondern bereits gerenderter Fließtext. `readTree` wirft
`JsonParseException` (empirisch bestätigt), der `catch` (`:187–188`) gibt zurück:

```java
return template.replace("{{input}}", inputJson);   // = das UNVERÄNDERTE Original-Template
```

Das Ergebnis des ersten Renderings wird verworfen:

```
Template (Config)   : Step: {{architect.output}} / Bewerte: {{reviewer.output}}
prompt2 (Engine :82): Step: PLAN-ABC            / Bewerte: {{reviewer.output}}
finale UserMessage  : Step: {{architect.output}} / Bewerte: {{reviewer.output}}
Enthaelt final 'PLAN-ABC'? false
```

**Aufgelöste Werte erreichen das Modell nicht.**

**Der vorhandene Integrationstest prüft das nicht:**
`E2E_DynamicWorkflowToolAndNameTest:225–237` validiert nur die **Config** (dass `userMessageTemplate`
da ist und `{{<predNode>.output}}` referenziert) — nie den gerenderten Prompt. Deshalb blieb der Defekt
unentdeckt.

**Änderung:** eines der beiden Renderings entfernen. Sinnvoll: das engine-seitige
(`InputBuilder.renderTemplate`) als einziges behalten, `PromptAssembler:122–127` auf den
Start-Envelope-Fall beschränken, wo `inputJson` tatsächlich JSON ist. Zusätzlich die Kopplung an
`isJsonOutputMode` auflösen, damit `userMessageTemplate` auch ohne `jsonOutput`/`jsonInput` greift:

```
nur userMessageTemplate               -> isJsonOutputMode=false -> Template-Pfad: NEIN
userMessageTemplate + jsonOutput=true -> isJsonOutputMode=true  -> Template-Pfad: JA
userMessageTemplate + jsonInput=true  -> isJsonOutputMode=true  -> Template-Pfad: JA
```

**Test (zuerst, schlägt heute fehl):** Workflow mit `userMessageTemplate` + `jsonOutput`, Executor
liefert `PLAN-ABC`. Assert auf den **finalen** UserMessage-Text.

**kafka-platform-Impact: keiner.** Der Worker setzt weder `userMessageTemplate` noch `jsonInput`
(`rg "setUserMessageTemplate|setJsonInput"` im Worker: null Treffer) und ruft `executeStructured`
statt des Template-Pfads. `WorkerAgentFactory:154` setzt nur `StructuredOutputConfig`.

---

### B2. Knotenbezogenes Exit-Prädikat (`exitConditionSource`) — **P0**

`LoopNodeExecutor:77–79` wertet `GraphUtils.combineOutputs(bodyOutputKeys, …)` aus — den
**Gesamtoutput aller Body-Knoten**. myclaw prüft `"OK".equals(reviewResult)`, also exakt und
knotenbezogen.

Empirisch gegen den echten `WorkflowCondition`:

```
Reviewer="OK", Designer-JSON mit "OK" darin                -> contains:OK = true   (richtig, aus dem falschen Grund)
Reviewer="KRITISCHE FEHLER: ...", Designer hat "Looks OK"  -> contains:OK = true   <-- FALSCH-POSITIV
beide Fälle                                                 -> equals:OK = false   <-- geht nie auf
```

`equals:OK` kann nie greifen, weil `combined` immer mit `[designer]: …\n\n[reviewer]: …` präfigiert ist.

**Änderung:** `exitConditionSource` einführen; leer = heutiges Verhalten (`combined`), gesetzt =
`outputs.get(source)`. Damit wird aus `contains:OK` die myclaw-Parität
`exitCondition: "equals:OK"` + `exitConditionSource: "reviewer"`.

**Test (schlägt heute fehl):** Reviewer sagt `KRITISCHE FEHLER`, Designer-Output enthält „OK". Assert:
Loop läuft **weiter**.

**kafka-platform-Impact: keiner.** Der Worker hängt nur an `quad-core` und `quad-tool-builtin`, nicht
am `quad-workflow-engine` (`rg "quad-workflow-engine" quad-workflow-worker/pom.xml`: null Treffer).

---

### B3. `outputHistory` im Run-Kontext — **P0**

`WorkflowExecutionContext:48` kennt nur `Map<String, String> outputs`, `putOutput:181–183`
überschreibt. Keine Historie, keine iterationsspezifischen Slots. Das ist die Wurzel: die
myclaw-Runde *N* kann die Reviewer-Response aus *N−1* nicht sehen, weil sie gelöscht wurde
(`LoopNodeExecutor:69–71`) und der Speicher nur einen Wert kennt.

**Änderung:**

```java
private final ConcurrentHashMap<String, List<OutputRecord>> outputHistory = new ConcurrentHashMap<>();
public record OutputRecord(int iteration, String nodeId, String value, String status) {}
```

`putOutput` schreibt **zusätzlich** mit der Iteration aus `loopIterations()` (`:51`, `:169–171`).
`outputs` bleibt „letzter Wert" — bestehende Workflows brechen nicht. `outputs.remove()` (`:156–158`,
aufgerufen von `LoopNodeExecutor:70`) muss die Historie **nicht** leeren.

**kafka-platform-Impact: keiner.** Wie B2.

---

### B4. Loop-Executor: Historie füllen, `retainOutput`, `aggregate` — **P0**

`LoopNodeExecutor:69–71` löscht die Body-Outputs vor jeder Iteration. Das Verhalten muss bleiben (es
verhindert Doppelläufe abgeschlossener Iterationen bei Resume), aber die Historie wird **vor** dem
Löschen gefüllt.

Zusätzlich: `config.retainOutput: true` lässt den Output in `outputs` stehen (Default `false` =
heutiges Verhalten), und die Loop-Ausgabe (`:85–89`) bekommt `aggregate: last | all | first`.

**kafka-platform-Impact: keiner.**

---

### B5. `inputMappings` mit `scope`-Auflösung — **P0**

Neu auf jedem Node ein generischer Supplement-Pfad zum kanten-gebundenen Mapping. `scope`-Werte:
`last`, `previousIteration`, `all`, `iteration:<n>`.

**Warum `InputBuilder` und nicht der Loop-Node:** der Fall ist nicht loop-spezifisch — derselbe Wunsch
besteht zwischen zwei `conditional`-Segmenten oder nach einem `join`.

**`format: "json"` über den bestehenden `JsonPathResolver.resolve` führen**, nicht über einen neuen
Mechanismus. `paths` ist bereits verdrahtet (`InputBuilder.jsonPaths:64–80`) und liefert
`[json: path '…' not resolved]` als Fehlermarker.

**kafka-platform-Impact: keiner.**

---

### B6. Unauflösbare Template-Placeholders benennen — **P1**

`renderTemplate:258–264` ist ein einfaches `String.replace`. Ein unauflösbarer Placeholder bleibt
**wörtlich** im Prompt:

```
Template : "Plan: {{architect.output}}\n\nFeedback:\n{{reviewer.output@previousIteration}}"
Ergebnis : "Plan: PLAN-A\n\nFeedback:\n{{reviewer.output@previousIteration}}"
```

Bei einem künftigen `@scope`-Syntax landen Tippfehler als Literaltext im Prompt, ohne Fehlerzeichen.
Der Resolver aus B5 braucht deshalb zwingend eine behandelte Fehlervariante.

**kafka-platform-Impact: keiner.**

---

### B7. `previousOutput`-Semantik klären — **P1**

`InputBuilder:249–253` berechnet `previousOutput` als Wert des **letzten Map-Keys**. `initialPrompt`
wird aber **nach** der Kanten-Schleife gesetzt (`:248`), also ist `lastKey` immer `initialPrompt` und
`previousOutput` immer `initialData`. Empirisch:

```
vars          = {architect.output=ARCHITEKTEN-PLAN-XYZ, initialPrompt=USER-REQUEST, previousOutput=USER-REQUEST}
zeigt auf Architekten-Plan? false
zeigt auf initialData?      true
```

Die Variable ist **kein** Vorgänger-Output. Jeder Nutzer, der sie als „letzter Vorgänger" versteht,
bekommt den Task-Text zurück.

**Änderung:** entscheiden — reparieren (letzter echter Kanten-Output) oder als deprecated markieren
und in `inputMappings` überführen. Mit B5 ist die zweite Variante die saubere.

**kafka-platform-Impact: keiner.**

---

### B8. Snapshot v3 inkl. `outputHistory` — **P0**

`snapshotJson:198` mit `SNAPSHOT_VERSION = 2` (`:192`) führt `loopIterations`, aber keine Historie.
Nach Kill/Resume wäre die Historie weg und Runde *N* startet wie Runde 1 — ein stilles
Feedback-Defekt-Fenster.

**Änderung:** `SNAPSHOT_VERSION = 3` + Restore-Pfad. V1/V2-Snapshots bleiben lesbar, dann ist
`outputHistory` leer → Verhalten wie heute.

**Test:** in `WorkflowExecutionContextSnapshotTest` (prüft `loopIterations` bereits) — Checkpoint in
Iteration 1, Resume, Iteration 2 → Feedback aus Iteration 1 noch da.

**kafka-platform-Impact: keiner.**

---

### B9. Conditional-Skip-Pfad abbilden — **P1**

myclaw überspringt den Reviewer bei Tool-Validierungsfehler (`DesignGraph:274–275`: `reviewerFeedback`
setzen + `continue`). Das ist eine *Ablehnungsentscheidung vor* dem Reviewer, kein Datenfluss-Problem.

Nicht gelöst durch diese Liste: die deterministische Tool-Validierung selbst
(`ToolValidationService`) — dafür bräuchte es einen Script-/Validation-Node-Typ, den es nicht gibt.

**kafka-platform-Impact: keiner.**

---

### B10. Neue Config-Felder ins Schema aufnehmen — **P2**

`quad.workflow.v1.schema.json` erlaubt für `node` und `edge` `additionalProperties: true` und kennt
**kein** `config`-Schema (nur `id`, `type`, `title`). `inputMappings` scheitert also nicht an der
Validierung — es bekommt aber auch keine Typsicherung. `QuadWorkflowSchemaValidator` erzwingt nichts
für `config`.

**Änderung:** `inputMappings`, `exitConditionSource`, `retainOutput`, `aggregate`, `inputMappings.scope`
ins Schema aufnehmen.

**kafka-platform-Impact: keiner.**

---

## C. Test-Infrastruktur — IST-Stand nach der Umsetzung

Die Nachprüfung hat **vier Testdateien gefunden, die in den beiden Analysen fehlten**, und **sechs
angelegt**. Dieser Abschnitt ist der Soll-Ist-Abgleich, keine Planung mehr.

### C1. Bestehende Abdeckung, die zuvor übersehen war

| Datei | Modul | Tests | Deckt ab |
|---|---|---|---|
| `quad-sandbox-docker/.../ReflectiveToolMethodTest.java` | quad-sandbox-docker | 2 | `ReflectiveToolMethod` + `BashSandboxTool` — **existiert seit Anfang an**, lag nur nicht in `quad-core` |
| `quad-quarkus/.../LoopNodeExecutorResumeTest.java` | quad-quarkus | 3 | **echter Loop mit Back-Edge**, Resume-Semantik, SQLite-Persistenz |
| `quad-quarkus/.../WorkflowExecutionContextSnapshotTest.java` | quad-quarkus | 11 | Snapshot v2, `loopIterations`, Prefix-Kette, Legacy-Kompatibilität |
| `quad-workflow-engine/.../context/InputBuilderTest.java` | engine | 5 | Kanten-Mapping, `inputs`-Liste, `JsonPathResolver` |
| `quad-workflow-engine/.../WorkflowConditionTest.java` | engine | 8 | `contains`/`equals`/`matches`/`iter*` |
| `quad-workflow-engine/.../WorkflowEngineContainerlessTest.java` | engine | 4 | containerlose Verdrahtung, Registry-Auflösung |

**Korrektur zu `myclaw2quad-reviewed.md` W5:** dort steht, es gebe „nur 4 `@Test` und keinen
Loop-Test". Richtig war: die vier Tests in `WorkflowEngineContainerlessTest` enthalten **keinen
Loop-Ausführungstest** — aber `LoopNodeExecutorResumeTest` ist genau einer, mit drei Fällen, und
läuft im Default-Build (kein `@Tag`). Die Test-Vorlage existierte also.

### C2. Angelegte Tests

| Datei | Tests | grün | `@Disabled` | Thema |
|---|---|---|---|---|
| `quad-core/.../tool/ReflectiveToolMethodBindingTest.java` | 11 | 9 | 2 | A1, A2, A3, A7, A12 |
| `quad-core/.../tool/QuadToolRegistryContractTest.java` | 11 | 11 | 0 | A10, A11 |
| `quad-core/.../tool/ToolActivatorToolErrorSemanticsTest.java` | 5 | 5 | 0 | A5 |
| `quad-core/.../session/CurrentSessionLifecycleTest.java` | 4 | 4 | 0 | A4 |
| `quad-core/.../agent/PromptAssemblerTemplateTest.java` | 10 | 10 | 0 | B1 |
| `quad-core/.../agent/AgentTemplatePipelineTest.java` | 5 | 5 | 0 | B1 |
| `quad-core/.../agent/runtime/ToolExecutorTest.java` | 7 | 7 | 0 | A6 |
| `quad-workflow-engine/.../LoopNodeExecutorExitScopeTest.java` | 10 | 10 | 0 | B2–B4 |
| `quad-workflow-engine/.../context/InputBuilderTemplateTest.java` | 15 | 15 | 0 | B5, B6, B7 |
| `quad-quarkus/.../WorkflowExecutionContextSnapshotTest.java` | +4 | +4 | 0 | B3, B8 |
| `kafka-platform/.../worker/tool/QuadIntegrationContractTest.java` | 8 | 8 | 0 | A1, A7, A10 |
| **Summe** | **90** | **88** | **2** | |

**Konvention:** Ein Test, der einen offenen Defekt aus TODO2.md beschrieb, hieß `defect_<ID>_…` und
trug `@Disabled`. Nach dem Beheben der Maßnahme wurde er umbenannt, die Assertion auf das Zielverhalten
umgestellt und die Annotation entfernt — er ist jetzt ein gewöhnlicher Regressionstest.

**Die zwei verbleibenden `@Disabled`** sind bewusst offen und keine Versäumnisse:

| Test | Frage |
|---|---|
| `openQuestion_A12_emptyDescription_whenToolAnnotationCarriesNoText` | Soll eine leere Beschreibung beim Start ein Fehler oder eine Warnung sein? Der Schema-Fix aus A2 löst das nicht. |
| `openQuestion_A3_stringParameterWithObjectArgument` | `convertValue` serialisiert ein Objekt für einen `String`-Parameter zu dessen JSON-Text (`{"x":1}`), statt es abzulehnen. Streng genommen sollte das ein Typfehler sein — die Entscheidung ist eine Vertragsfrage, keine Implementierungsfrage. |

### C3. Geänderte bestehende Dateien

`quad-core/.../agent/runtime/ToolExecutorTest.java` — `idempotent_tool_replays_cached_result` setzte
`@Idempotent` auf `spec()` und zementierte den Defekt aus A6. Der Test wurde zu
`handWrittenToolWithIdempotentMeta_replaysCachedResult` umgeschrieben: er überschreibt jetzt `meta()`
statt zu annotieren, was der vorgesehene Weg für handgeschriebene Tools ist. Neu daneben:
`idempotent_annotationOnToolMethod_shouldActivateTheCache`, `metaCarriesIdempotency…`,
`metaCarriesDescription…`.

`quad-quarkus/.../WorkflowExecutionContextSnapshotTest.java` — prüfte fest `version == 2`. Auf 3
gehoben und um vier Fälle erweitert: `outputHistorySurvivesASnapshotRoundTrip`,
`previousIterationResolvesThePriorRoundsValue`, `clearOutputKeepsTheHistory`,
`legacySnapshotWithoutOutputHistoryRestoresOutputsOnly`.

### C4. Befund aus dem Test-Design — jetzt behoben

Beim Schreiben des Worker-Tests kam ein Effekt zum Vorschein, der in keiner Analyse stand:

```java
// ReflectiveToolMethod, vorher
CurrentSession.setCurrent(state);          // überschreibt mit dem Argument
Object[] args = buildArgs(...);            // :143 liest CurrentSession.getCurrent() zurück
```

`execute(jsonArgs, null)` **zerstört** einen zuvor gesetzten ThreadLocal. `execute` speichert und
stellt jetzt in `finally` wieder her. Der Test heißt
`executeWithNullStateKeepsTheExistingThreadLocalBinding`.

### C5. Noch nicht abdeckbar

| Fall | Grund |
|---|---|
| T14 Prompt-Golden ohne die neuen Felder | braucht eine Golden-Datei als Ausgangsbasis; es gibt keine |
| e2e-Tests T9–T12 | bleiben `@Tag("e2e")` und laufen nur mit `-Pintegration-tests`. Containerlose Varianten von T9 (B1) und T11/B2 gibt es seit dieser Fassung |

---

## D. Auswirkungen auf kafka-platform

### D1. Abhängigkeitslage

`quad-workflow-worker/pom.xml` hängt an **zwei** quad-Artefakten:

```
de.augmentia.quad:quad-core:1.0.0-SNAPSHOT
de.augmentia.quad:quad-tool-builtin:1.0.0-SNAPSHOT
```

**Nicht** am `quad-workflow-engine`. Damit gilt:

| Änderung | kafka-platform |
|---|---|
| A1–A12 (alle `quad-core`) | umgesetzt, **kein Verhaltensbruch** — durch `QuadIntegrationContractTest` abgesichert |
| B1–B10 (alle `quad-workflow-engine`, `PromptAssembler`) | **nicht betroffen** |
| C (Test-Infrastruktur) | Test `QuadIntegrationContractTest` ergänzt |

Ausnahme: `PromptAssembler` liegt in `quad-core`, nicht im Engine-Modul. B1 berührt damit
quad-core — aber der Worker erreicht den Pfad nicht (siehe D3).

### D2. Verwendete quad-Symbole im Worker

32 deduplizierte Referenzen, alle aus `quad-core` und `quad-tool-builtin`:

```
agent:            Agent, AgentResult
agent.runtime:    AgentEventPublisher, IdempotencyStore, OTelAgentTracer, ToolExecutor
annotation:       Tool
capability:       ContextManager, Skill, SkillParser, TextSkill
config:           StructuredOutputConfig
hook:             HookRegistry, AgentSkillsPlugin
security:         SecurityContext
session:          AgentSessionState, CurrentSession, SessionMemory, WorkspaceResolver
tool:             QuadToolRegistry, ReflectiveToolMethod, ToolArgsMapper, ToolMethod, SkillLoadTool
tool.builtin:     ReadFileTool, WriteTool, GrepTool, LsTool, FindTool, WebSearchTool, WebFetchTool
```

Nicht verwendet: `ToolActivatorTool`, `GizmoInvoker`, `DynamicToolProvider`, `getSpecifications`,
`ToolResult.json`, `@Hidden`, `@GenerationMethod`, `@ToolRole`, `@SagaAgent`, `@Idempotent`.

### D3. Punkt-für-Punkt — Prognose, nachträglich bestätigt

| ID | Betroffen? | Begründung |
|---|---|---|
| **A1** `-parameters` | **bereits erledigt** | `quad-workflow-worker/pom.xml:194` setzt `<parameters>true</parameters>`. Der Worker baut seine eigenen Klassen also mit Namen. Die Änderung in quad wirkt **nur** auf quad-eigene Klassen — und die haben keine `@Tool`-Methoden (`rg "@Tool" quad-core/src/main`: null Treffer außer Annotation/ReflectiveToolMethod/SubAgentTool). **A1 ist für den Worker ein Nichtfall, für quad aber Voraussetzung.** |
| **A2** `parameters = null` | **ja, latent** | `LsTool` ist in `QuadWorkerConfig:123–126` registriert. Der Defekt war produktiv vorhanden und ist mit der Umsetzung behoben. |
| **A3** Binding | **nein** | Die sieben registrierten Tools haben ausschließlich `String`/`Integer`/`Boolean`-Parameter. Kein `List`, `Map` oder Record. |
| **A4** ThreadLocal | **nein** | `TaskExecutor:216–221` bindet selbst und räumt auf. Zusätzlich ist der quad-seitige save/restore-Fix implementiert; `QuadIntegrationContractTest.executeWithNullStateKeepsTheExistingThreadLocalBinding` bestätigt, dass `execute(…, null)` die Worker-Bindung nicht zerstört. |
| **A5** `ToolActivatorTool` | **nein** | Im Worker nicht referenziert. |
| **A6** Idempotenz | **nein** | Keine `@Idempotent`-Annotation im Worker. `IdempotencyStore` wird verdrahtet (`WorkerAgentFactory:138`), aber nie über den Tool-Pfad mit Key genutzt. |
| **A7** `state` statt ThreadLocal | **umgesetzt, ohne Bruch** | Genau die empfohlene Variante: `state` hat Vorrang, der ThreadLocal bleibt als Fallback bestehen, damit die sieben Builtins mit `currentState()` weiter funktionieren. `QuadIntegrationContractTest` prüft beide Richtungen. |
| **A8** `ToolInvocation` | **nein** | Der Worker **implementiert** `ToolMethod` nicht, er erzeugt Instanzen über `ReflectiveToolMethod` (`ToolCatalog:31`). Eine `execute`-Signaturänderung ist quellspezifisch unkritisch. `ToolCatalogTest` prüft nur `spec()`. |
| **A9** `ToolMeta` | **umgesetzt** | `ToolCatalog:27` prüft weiterhin `isAnnotationPresent(Tool.class)` vor der Konstruktion — das bleibt gültig, weil `ReflectiveToolMethod` die Metadaten beim Konstruieren materialisiert. Der Worker braucht keine Änderung. |
| **A10** Registry | **umgesetzt, unkritisch** | `register` wirft jetzt bei Kollision. `AgentToolRegistryFactory:28–37` registriert nur agenteneigene und freigegebene Tools — keine Kollision möglich. `ToolCatalog.require()` wirft bereits vorher fail-fast; der Test `unknownGlobalTool_throwsFromTheCatalogDuringRegistryConstruction` hält die Reihenfolge fest. |
| **A11** Stubs entfernen | **nein** | Im Worker nicht referenziert. |
| **A12** Validierung | **indirekt** | Fehler beim Start wären ein Bean-Startup-Problem in `QuadWorkerConfig:144–158`. Als Warnung loggen, **nicht** werfen. |
| **B1** Rendering | **nein** | Der Worker setzt weder `userMessageTemplate` noch `jsonInput` und ruft `executeStructured` (`TaskExecutor:218`). `PromptAssembler` wird nicht erreicht — nach der Umstellung erst recht nicht, weil der Nicht-JSON-Prompt jetzt unberührt durchläuft. |
| **B2–B10** | **nein** | Alle im `quad-workflow-engine`, das der Worker nicht einbindet. |

### D4. Empirischer Nachweis für den empfindlichsten Punkt

Der Worker-Test `JdbcAgentConfigSourceE2ETest:163–172` definiert Tools **ohne** `@Param`:

```java
public static class ReadFileTool {
    @Tool("Read a file from workspace")
    public String readFile(String path) { return "content of " + path; }
}
```

Das ist genau das Muster, an dem A1 sichtbar wird. Mit `-parameters` (so baut der Worker) gegen den
echten `ReflectiveToolMethod`:

```
--- MIT -parameters (so baut kafka-platform den Worker) ---
Spec-Parameter : {path=JsonStringSchema, required=[path]}
Aufruf mit {"path":"a.txt"} -> content of a.txt     <-- korrekt
```

Ohne `-parameters` wäre es `arg0` und der Aufruf ergäbe `content of null` — **ohne Exception**. Der
Worker ist davor geschützt, weil sein POM den Compiler-Arg setzt. Das bestätigt A1 als quad-seitige
Lücke und zeigt gleichzeitig, dass die Absicherung in quad selbst fehlt: quad-core und
quad-tool-builtin haben sie nicht.

### D5. Ablauf, der sich bewährt hat

Nach jedem quad-Schritt: `mvn -DskipTests install` in quad, dann `mvn test` in kafka-platform. Der
Worker zieht die SNAPSHOT-Artefakte aus dem lokalen Repository, nicht aus dem Quellbaum — ohne
`install` prüft man die alten Klassen. Genau so wurde vorgegangen, und `QuadIntegrationContractTest`
läuft dadurch gegen das geänderte Artefakt.

`scripts/build-deps-tarball.sh` vergleicht zusätzlich Zeitstempel und bricht bei veralteten Artefakten
ab. Das fängt einen vergessenen `install` ab, ersetzt aber keine gezielte Prüfung, weil es keine
API-Kompatibilität prüft.

**Für künftige Eingriffe:** der Worker braucht Code-Änderungen nur, wenn eine quad-Maßnahme eine
Signatur ändert, die er benutzt (`ToolMethod.execute`, `QuadToolRegistry.register`,
`ReflectiveToolMethod`-Konstruktor). `QuadIntegrationContractTest` deckt genau diese drei ab — dort
sollte ein neuer Umbau zuerst einen Test verlieren.

---

## E. Konsolidierte Reihenfolge

| # | Maßnahme | Modul | Prio | Aufwand | Status |
|---|---|---|---|---|---|
| 1 | **B1** Render-Strecke auflösen, Template-Pfad an `isJsonOutputMode` entkoppeln | quad-core | **P0** | M | ✅ umgesetzt |
| 2 | **A1** `-parameters` in quad setzen | alle 10 Module | **P0** | S | ✅ umgesetzt |
| 3 | **A2** `.parameters()` immer setzen | quad-core | **P0** | S | ✅ umgesetzt |
| 4 | **A3** Binding auf `convertValue` | quad-core | **P0** | M | ✅ umgesetzt |
| 5 | **A4** ThreadLocal save/restore statt set/null | quad-core | **P0** | S | ✅ umgesetzt |
| 6 | **A5** `ToolActivatorTool` Fehlerpfade | quad-core | **P0** | S | ✅ umgesetzt |
| 7 | **A6** Idempotenz auf `ToolMeta` | quad-core | **P0** | S | ✅ umgesetzt |
| 8 | **B2** `exitConditionSource` | engine | **P0** | S | ✅ umgesetzt |
| 9 | **B3** `outputHistory` + `OutputRecord` | engine | **P0** | S | ✅ umgesetzt |
| 10 | **B4** Loop: Historie füllen, `retainOutput`, `aggregate` | engine | **P0** | S | ✅ umgesetzt |
| 11 | **B5** `inputMappings` + `scope`-Resolver | engine | **P0** | M | ✅ umgesetzt |
| 12 | **B8** Snapshot v3 | engine | **P0** | S | ✅ umgesetzt |
| 13 | **C** Testbasis | quad-core, engine, worker | **P0** | M | ✅ umgesetzt, Abschnitt C |
| 14 | **B6** Placeholder-Fehlerbehandlung | engine | P1 | S | ✅ umgesetzt (`renderTemplateStrict`) |
| 15 | **A7** `state` als Parameter, ThreadLocal als Fallback | quad-core | P1 | M | ✅ umgesetzt (Argument hat Vorrang, ThreadLocal bleibt) |
| 16 | **A9** `ToolMeta` | quad-core | P1 | M | ✅ umgesetzt, Annotationen `@Idempotent`/`@Hidden`/`@ToolRole` verdrahtet |
| 17 | **A10** Registry-Vertrag härten | quad-core | P1 | S | ✅ umgesetzt |
| 18 | **A11** Stubs entfernen | quad-core | P1 | S | ✅ umgesetzt (`GizmoInvoker` gelöscht, `createToolInvoker` wirft) |
| 19 | **B7** `previousOutput`-Semantik | engine | P1 | S | ✅ umgesetzt |
| 20 | **B9** Conditional-Skip-Pfad | engine | P1 | M | offen |
| 21 | **A8** `ToolInvocation`-Context | quad-core | P1 | M | offen, bewusst hinten |
| 22 | **A12** Beschreibungsvalidierung (nur Warnung) | quad-core | P2 | S | offen |
| 23 | **B10** Schema erweitern | workflow-core | P2 | S | offen |
| 24 | **T11, T13, T14** verbleibende e2e-Tests | engine | P2 | M | T13 neu, T11/T14 nach B5 möglich |

**Reihenfolge-Begründung:** 1 vor allem, weil jede Template-Arbeit (11, 14) sonst gegen eine Strecke
gebaut würde, die ihr Ergebnis verwirft. 2 vor 3–7, weil ohne `-parameters` jeder namensbasierte
Fix eine Zeitbombe ist. 8–12 als Block, weil sie nur zusammen eine nutzbare Fähigkeit ergeben — einzeln
ist `outputHistory` (9) tote Datenhaltung und `inputMappings` (11) ein Resolver ohne Daten.

**Position 13 ist mit Abschnitt C erledigt.** 65 Tests angelegt, davon 21 `@Disabled` mit Defekt-ID.
Damit ist für jede Maßnahme 1–12 bereits ein Test vorhanden, der nach der Korrektur schlagendätig
wird. Das ändert die Reihenfolge nicht, aber es macht jede Zeile einzeln prüfbar: kein Fix ohne
Testnachweis.

Position 21 (`ToolInvocation`) steht bewusst hinten: sie ändert eine Schnittstelle, die in quad vier
Implementierungen hat, und setzt eine korrekte Datenebene voraus (4–7, 9–11).

---

## F. Offene Punkte

### Offen aus dem P0-Block

- **A12 — leere Tool-Beschreibung.** `@Tool` ohne Text erzeugt weiterhin `description = ""`. Das Modell
  bekommt namenlose Werkzeuge. Der Fix ist eine Validierung beim Agent-Start, kein Schema-Fix. Als
  **Warnung** loggen, nicht werfen: `QuadWorkerConfig:144–158` baut den `ToolCatalog` beim Start, und
  eine geworfene Validierung macht den Worker startunfähig. Betrifft keine der neun Builtins.
- **A3 — Objekt für `String`-Parameter.** `convertValue` serialisiert ein Objekt zu dessen JSON-Text
  (`got:[{"x":1}]`) statt es abzulehnen. Streng genommen ein Typfehler; welche Semantik gilt, ist eine
  Vertragsentscheidung.
- **B9 — Conditional-Skip-Pfad.** myclaw überspringt den Reviewer bei Tool-Validierungsfehler
  (`DesignGraph:274–275`). Nicht umgesetzt; braucht eine eigene Node-Art für die Validierung.

### Nicht in dieser Liste

- Die Tool-Validierung aus myclaws `ToolValidationService` — dafür fehlt ein Node-Typ. Eigenes Thema.
- strands' `executeAll`-Parallelität: dortige Entscheidung, kein quad-Defekt.
- **A8** (`ToolInvocation`-Context): bewusst hinten, weil es eine Schnittstelle ändert, die in quad
  vier Implementierungen hat, und eine korrekte Datenebene voraussetzt — die liegt jetzt vor.

### Verifikationsstand

Die P0-Maßnahmen 1–12 sind durch Tests belegt und die Testzahlen in Abschnitt C stimmen mit einem
`mvn clean test` in beiden Repos überein. Für `kafka-platform` wurde zusätzlich
`mvn -DskipTests install` in quad ausgeführt, damit der Worker gegen das **geänderte** Artefakt testet
statt gegen das alte aus dem lokalen Repository.

`AgentNodeExecutor` hat weiterhin keinen eigenen Test; die B1-Kette wird in
`AgentTemplatePipelineTest` von Hand nachgestellt. Das ist die einzige Lücke zwischen Test und
Produktivpfad, die ich bewusst nicht geschlossen habe.
---

## G. Integrationstests — eigener Strang

Der P0-Block oben ist durch Unit- und Komponententests belegt. Er sagt nichts darüber, ob das
Ganze im Betrieb zusammenspielt. Dafür gibt es jetzt ein eigenes Modul: `integration-tests/`.

```
L0  PlatformContractTest        18 Tests   Verträge, ohne Netzwerk
L1  OrchestratorApiTest         14 Tests   Orchestrator-REST
L2  KafkaContractTest           11 Tests   Broker, 1 @Disabled
L3  PlatformEndToEndTest         8 Tests   echte Läufe inkl. HITL und Abbruch
L3  CrossPlatformWorkflowTest    7 Tests   quad → Bridge → kafka, 1 @Disabled
──────────────────────────────────────────────────────────────────────────────────
    58 Tests, 0 Fehler, 2 dokumentierte Befunde
```

Ausführbar mit einem Befehl, inklusive Stack-Start und -Stopp:

```bash
cd integration-tests && ./scripts/run-integration-tests.sh e2e
```

Getestet wird gegen gepackte JARs, echten Broker und echte Postgres. Zwei der Befunde stammen
aus diesem Strang und sind dort vollständig beschrieben — siehe `integration-tests/README.md`.

### G1 — Topics entstehen in der Reihenfolge des ersten Zugriffs

`KafkaTopicInitializer` legt nur fehlende Topics an und gleicht bestehende nicht ab. Wer zuerst
publiziert, legt damit Partitionen und `cleanup.policy` fest. Auf einem wiederverwendeten Broker
stehen `agent.execution.intake` und `agent.execution.results` mit **1** statt 12 Partitionen und
`agent.config.updates` mit `delete` statt `compact`; `agent.tasks.*` (12), `control.timeouts` (6)
und `execution.dlq` (6) stimmen, weil sie beim ersten Start noch nicht existierten.

Nicht kosmetisch: mit einer Partition läuft die Plattform serialisiert. Die Tests fallen nicht,
es wird nur langsamer.

Fix-Richtung: `createPartitions` und `alterConfigs` statt `createTopics` — beides idempotent.
Test: `brokerMatchesTopicSpecification` (`@Disabled`), mit grünem Ist-Gegenstück
`driftIsLimitedToTopicsCreatedByTheBroker`.

### G2 — quad meldet einen leeren Workflow als `completed`

`POST /api/ui/workflows` mit `nodes: []` wird angenommen, der Lauf endet mit `completed`. Damit
signalisiert der Status nichts — es lief nichts. Die Validierung gehört ins Anlegen oder den Start.

Test: `quadRejectsEmptyWorkflow` (`@Disabled`), Ist-Gegenstück
`emptyWorkflowRunsAndReportsCompleted`.

### G3 — Konfiguration, die man beim Einrichten sucht

Keine Fehler, aber jeweils mit Suchzeit verbunden:

- **`quarkus.base-url`** des Orchestrators steht auf `http://localhost:8086`. Der Proxy-Pfad der
  Bridge (`X-Backend: quarkus`) liefert damit 502, wenn quad woanders läuft. Der Test-Runner
  setzt das Argument.
- **Worker-Port** ist in `quad-workflow-worker/src/main/resources/application.yml` fest auf `8092`
  verdrahtet, ohne `${...}` — also nicht per Environment überschreibbar, nur per
  `--server.port`.
- **quad-Datenverzeichnis** löst `data/quad.db` gegen das Arbeitsverzeichnis auf, nicht gegen das
  Jar. Aus dem Modulverzeichnis gestartet startet quad nicht.
- **`agent-worker` hört nicht mehr auf `agent.tasks.*`**, sondern auf `legacy\.agent\.tasks\..*`.
  Der kanonische Worker ist `quad-workflow-worker`. Wer den Legacy-Worker startet, bekommt einen
  stillen Stack und Läufe, die hängen bleiben.
