package de.augmentia.agentflow.app.testbench;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Runs a single agent with a diagnostic trace (tokens, tools, cost, provenance). */
@RestController
public class TestbenchController {

    private final TestbenchService service;

    public TestbenchController(TestbenchService service) {
        this.service = service;
    }

    @PostMapping("/agentflow/testbench/run")
    public Map<String, Object> run(@RequestBody TestbenchRequest request) {
        return service.run(request);
    }
}
