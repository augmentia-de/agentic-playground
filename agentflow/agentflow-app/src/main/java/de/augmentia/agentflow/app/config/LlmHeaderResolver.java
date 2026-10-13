package de.augmentia.agentflow.app.config;

import java.util.Map;

/** Resolves the HTTP headers to attach to an LLM request (order preserved). */
@FunctionalInterface
public interface LlmHeaderResolver {

    Map<String, String> resolve();
}
