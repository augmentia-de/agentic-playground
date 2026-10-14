package de.augmentia.quad.quarkus.persistence;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class JournalServiceTest {

    @Test
    void defaultPathIsExpanded() {
        JournalService js = new JournalService();
        Path p = js.path();
        assertFalse(p.toString().contains("${"));
        assertFalse(p.toString().contains("QUAD_JOURNAL_PATH"));
        assertEquals("logs/journal.jsonl", p.toString());
    }
}
