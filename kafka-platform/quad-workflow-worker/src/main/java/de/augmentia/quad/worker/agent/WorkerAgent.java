package de.augmentia.quad.worker.agent;

import de.augmentia.quad.core.agent.Agent;
import de.augmentia.quad.core.session.AgentSessionState;
import de.augmentia.quad.core.session.CurrentSession;
import de.augmentia.quad.core.session.WorkspaceResolver;
import dev.langchain4j.model.chat.ChatModel;

/**
 * Custom worker agent that extends the quad-core agent base.
 *
 * IMPORTANT: no CDI dependencies - everything is wired manually.
 * A new instance is created per task (no singleton agents!).
 */
public class WorkerAgent extends Agent {

    @Override
    protected AgentSessionState newSessionState() {
        return new AgentSessionState();
    }

    /**
     * Manual injection of CurrentSession and WorkspaceResolver.
     * Corresponds to the @Inject fields in super(Agent).
     */
    public void wire(CurrentSession currentSession, WorkspaceResolver workspaceResolver) {
        this.currentSession = currentSession;
        this.workspaceResolver = workspaceResolver;
    }

    /**
     * Sets the LLM model for this agent (alias for setLlm).
     */
    public void setChatModel(ChatModel model) {
        setLlm(model);
    }
}