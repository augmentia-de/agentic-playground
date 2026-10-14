package de.augmentia.quad.quarkus.ui;

import de.augmentia.quad.contract.ApiDtos;
import de.augmentia.quad.quarkus.persistence.SqlDialect;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

import java.nio.file.Path;
import java.sql.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PersistenceStoreTest {

    @TempDir Path tempDir;
    PersistenceStore store;
    SQLiteDataSource ds;

    @BeforeEach
    void setUp() throws Exception {
        SQLiteDataSource d = new SQLiteDataSource(new SQLiteConfig());
        d.setUrl("jdbc:sqlite:" + tempDir.resolve("t.db"));
        ds = d;
        store = new PersistenceStore();
        store.setDataSource(d);
        var dialect = new SqlDialect();
        dialect.setDbKind("sqlite");
        store.setDialect(dialect);
        try (var c = d.getConnection(); var s = c.createStatement()) {
            s.execute("CREATE TABLE agents (" +
                "id text primary key, name text, description text, category text, model text, " +
                "temperature real, max_tokens int, top_p real, tools text, guardrails_input text, " +
                "guardrails_output text, hooks text, agent_type text, system_prompt text, " +
                "user_message_template text, json_output int, json_output_schema text, chat_parameters text, " +
                "active int, timeout_seconds int, created_at timestamp, updated_at timestamp)");
            s.execute("CREATE TABLE workflows (" +
                "id text primary key, name text, initial_task text, nodes text, edges text, " +
                "cache_prefix text, prefix_caching int, created_at text, updated_at text)");
        }
    }

    @Test
    void arrayWithComma_roundTrips() {
        var a = new ApiDtos.AgentDefinition();
        a.id = "a1"; a.name = "A1"; a.tools = new String[]{"x,y", "a b"};
        store.upsertAgent(a);
        var loaded = store.loadAllAgents();
        assertEquals(1, loaded.size());
        assertArrayEquals(new String[]{"x,y", "a b"}, loaded.get("a1").tools);
    }

    @Test
    void arrayEmptyAndSpecialChars() {
        var a = new ApiDtos.AgentDefinition();
        a.id = "a2"; a.name = "A2"; a.tools = new String[0];
        a.hooks = new String[]{"h\"q", "line\nbreak"};
        store.upsertAgent(a);
        var loaded = store.loadAllAgents().get("a2");
        assertNotNull(loaded);
        assertEquals(0, loaded.tools.length);
        assertArrayEquals(new String[]{"h\"q", "line\nbreak"}, loaded.hooks);
    }

    @Test
    void oneBadRow_othersStillLoad() throws Exception {
        // Good workflow
        var wf = new ApiDtos.WorkflowDef();
        wf.id = "goodwf"; wf.name = "Good"; wf.nodes = new java.util.ArrayList<>(); wf.edges = new java.util.ArrayList<>();
        store.upsertWorkflow(wf);
        // Bad workflow: malformed nodes JSON
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.execute("INSERT INTO workflows (id, name, nodes, edges, created_at, updated_at) VALUES " +
                "('badwf', 'Bad', '{not json', '[]', '2020-01-01', '2020-01-01')");
        }
        var loaded = store.loadAllWorkflows();
        assertTrue(loaded.containsKey("goodwf"), "valid workflow must survive the bad row");
    }
}
