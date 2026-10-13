package de.augmentia.agentflow.examples.onboarding;

import de.augmentia.agentflow.examples.Examples;
import de.augmentia.quad.core.session.memory.MemoryCategory;
import de.augmentia.quad.core.session.memory.MemoryEntry;
import de.augmentia.quad.core.session.memory.PersistentMemoryStore;
import de.augmentia.quad.core.tool.SseMcpProxy;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;
import java.util.stream.Collectors;
import io.temporal.spring.boot.ActivityImpl;

/** Onboarding assistant: long-term memory + capability lookup + optional MCP tools. */
@Component
@ActivityImpl(taskQueues = "agentflow")
public class OnboardingActivitiesImpl implements OnboardingActivities {

    private final PersistentMemoryStore memory;

    public OnboardingActivitiesImpl(PersistentMemoryStore memory) {
        this.memory = memory;
    }

    @Override
    public String recallProject(String projectScope) {
        return memory.formatForInjection(projectScope);
    }

    @Override
    public String rememberProject(String projectScope, String fact) {
        String id = "onboarding-" + UUID.randomUUID();
        memory.remember(new MemoryEntry(id, projectScope, fact, fact,
                MemoryCategory.PROJECT_FACT, Instant.now(), false));
        return id;
    }

    @Override
    public String planOnboarding(String newHire, String memoryBlock) {
        String prompt = "New hire: " + newHire + "\n"
                + "Known project context:\n" + memoryBlock + "\n\n"
                + "Checklist to cover: " + String.join("; ", OnboardingPolicy.firstWeekChecklist());
        return Examples.ask(Examples.agent(
                "You are an onboarding assistant. Produce a concrete, day-by-day first-week plan."), prompt);
    }

    @Override
    public String listMcpTools(String serverUrl) {
        if (serverUrl == null || serverUrl.isBlank()) {
            return "";
        }
        return new SseMcpProxy(serverUrl, "agentflow").loadTools().stream()
                .map(t -> t.spec().name())
                .collect(Collectors.joining(","));
    }
}
