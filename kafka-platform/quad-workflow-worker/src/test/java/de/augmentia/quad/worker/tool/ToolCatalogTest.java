package de.augmentia.quad.worker.tool;

import de.augmentia.quad.core.annotation.Tool;
import de.augmentia.quad.core.tool.ToolArgsMapper;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ToolCatalogTest {

    @Test
    void catalogContainsAllBuiltinTools() {
        ToolArgsMapper argsMapper = new ToolArgsMapper(new com.fasterxml.jackson.databind.ObjectMapper());
        
        Set<Object> testTools = Set.of(
            new TestReadFileTool(),
            new TestWriteTool()
        );
        
        ToolCatalog catalog = new ToolCatalog(argsMapper, testTools);
        
        assertThat(catalog.names()).contains("readFile", "write");
        assertThat(catalog.names()).hasSize(2);
    }

    @Test
    void requireReturnsToolByName() {
        ToolArgsMapper argsMapper = new ToolArgsMapper(new com.fasterxml.jackson.databind.ObjectMapper());
        
        Set<Object> testTools = Set.of(new TestReadFileTool());
        
        ToolCatalog catalog = new ToolCatalog(argsMapper, testTools);
        
        var tool = catalog.require("readFile");
        
        assertThat(tool).isNotNull();
        assertThat(tool.spec().name()).isEqualTo("readFile");
    }

    @Test
    void requireThrowsForUnknownTool() {
        ToolArgsMapper argsMapper = new ToolArgsMapper(new com.fasterxml.jackson.databind.ObjectMapper());
        
        Set<Object> testTools = Set.of(new TestReadFileTool());
        
        ToolCatalog catalog = new ToolCatalog(argsMapper, testTools);
        
        assertThatThrownBy(() -> catalog.require("unknownTool"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown tool");
    }

    @Test
    void tryGetReturnsNullForUnknownTool() {
        ToolArgsMapper argsMapper = new ToolArgsMapper(new com.fasterxml.jackson.databind.ObjectMapper());
        
        Set<Object> testTools = Set.of(new TestReadFileTool());
        
        ToolCatalog catalog = new ToolCatalog(argsMapper, testTools);
        
        assertThat(catalog.tryGet("nonexistent")).isNull();
    }

    @Test
    void containsChecksExistence() {
        ToolArgsMapper argsMapper = new ToolArgsMapper(new com.fasterxml.jackson.databind.ObjectMapper());
        
        Set<Object> testTools = Set.of(new TestReadFileTool());
        
        ToolCatalog catalog = new ToolCatalog(argsMapper, testTools);
        
        assertThat(catalog.contains("readFile")).isTrue();
        assertThat(catalog.contains("nope")).isFalse();
    }

    // ── Test Tools ──

    public static class TestReadFileTool {
        @Tool("Read a file from workspace")
        public String readFile(String path) {
            return "content of " + path;
        }
    }

    public static class TestWriteTool {
        @Tool("Write content to file")
        public String write(String path, String content) {
            return "wrote to " + path;
        }
    }
}