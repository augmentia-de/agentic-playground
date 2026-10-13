package de.augmentia.agentflow.app.web;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** Read API over Temporal's visibility store: list runs, inspect a run and (admin) terminate. */
@RestController
public class RunsController {

    private final RunViews runs;

    public RunsController(RunViews runs) {
        this.runs = runs;
    }

    @GetMapping("/agentflow/runs")
    public List<Map<String, Object>> list(@RequestParam(name = "query", defaultValue = "") String query,
                                          @RequestParam(name = "limit", defaultValue = "50") int limit) {
        return runs.list(query, limit);
    }

    @GetMapping("/agentflow/runs/{workflowId}")
    public Map<String, Object> detail(@PathVariable("workflowId") String workflowId) {
        return runs.detail(workflowId);
    }

    @GetMapping("/agentflow/runs/{workflowId}/events")
    public List<Map<String, Object>> events(@PathVariable("workflowId") String workflowId,
                                            @RequestParam(name = "max", defaultValue = "500") int max) {
        return runs.events(workflowId, max);
    }

    @PostMapping("/agentflow/runs/{workflowId}/terminate")
    public Map<String, Object> terminate(@PathVariable("workflowId") String workflowId,
                                         @RequestParam(name = "reason", defaultValue = "terminated via UI") String reason) {
        runs.terminate(workflowId, reason);
        return Map.of("workflowId", workflowId, "terminated", true, "reason", reason);
    }
}
