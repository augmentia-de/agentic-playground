package de.augmentia.agentplatform.common.agent;

import de.augmentia.agentplatform.common.event.AgentTaskEvent;

public interface AgentExecutor {

    String agentType();

    AgentExecutionResult execute(AgentTaskEvent task);
}
