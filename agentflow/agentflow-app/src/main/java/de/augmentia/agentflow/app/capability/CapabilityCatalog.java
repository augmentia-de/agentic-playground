package de.augmentia.agentflow.app.capability;

import de.augmentia.quad.core.capability.Capability;

import java.util.List;
import java.util.Set;

/**
 * The catalog of capabilities (tools/skills) that is indexed for retrieval.
 * Kept as plain data so it can be reused by the bootstrap and by tests.
 */
public final class CapabilityCatalog {

    private CapabilityCatalog() {
    }

    public static List<Capability> all() {
        return List.of(
                cap("readFile", "Read a file from the workspace", "quad.builtin.ReadFileTool::read", "FILE"),
                cap("writeFile", "Write content to a file in the workspace", "quad.builtin.WriteTool::write", "FILE"),
                cap("listDirectory", "List directory entries in the workspace", "quad.builtin.LsTool::list", "FILE"),
                cap("grepSearch", "Search file contents with a regular expression", "quad.builtin.GrepTool::grep", "FILE"),
                cap("findFiles", "Find files by name pattern", "quad.builtin.FindTool::find", "FILE"),
                cap("webSearch", "Search the web for current information", "quad.builtin.WebSearchTool::search", "WEB"),
                cap("webfetch", "Fetch and read the contents of a web page", "quad.builtin.WebFetchTool::fetch", "WEB"),
                cap("capability_search", "Analyze a task and recommend matching tools and skills",
                        "quad.tool.CapabilitySearchTool::execute", "META"),
                cap("now", "Return the current server time in ISO-8601", "agentflow.tools.ClockTool::now", "UTIL"),
                cap("mcp_tools", "Call tools exposed by an external MCP server over SSE",
                        "quad.tool.SseMcpProxy::loadTools", "MCP"),
                cap("subagent", "Delegate a sub-task to a specialised sub-agent",
                        "quad.tool.SubAgentTool::execute", "AGENT")
        );
    }

    private static Capability cap(String name, String description, String methodRef, String type) {
        return new Capability(name, description, methodRef, "quad", type, Set.of(), 0.5);
    }
}
