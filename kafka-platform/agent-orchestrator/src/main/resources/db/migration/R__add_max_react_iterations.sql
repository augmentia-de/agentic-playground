-- ReAct iteration limit per agent.
--
-- Up to here there was no column for it: the worker hardcoded the limit to 10
-- (JdbcAgentConfigSource.mapRow), regardless of which agent was running. An agent that
-- needs a flat loop could not be shortened, one with long
-- chains could not be extended.
--
-- The default 10 corresponds to what was previously hardcoded, so that existing
-- deployments keep their behaviour. Quad can set the field; the orchestrator
-- supplies app.agents.default-max-react-iterations as a fallback.
ALTER TABLE agent_definitions
    ADD COLUMN IF NOT EXISTS max_react_iterations INT NOT NULL DEFAULT 10;

-- CHECK instead of guarding in the code: quad rejects 0 (Agent.setReActMaxIterations),
-- so the DB should not accept a meaningless limit in the first place.
ALTER TABLE agent_definitions
    DROP CONSTRAINT IF EXISTS ck_agent_definitions_react_iterations_positive;
ALTER TABLE agent_definitions
    ADD CONSTRAINT ck_agent_definitions_react_iterations_positive
    CHECK (max_react_iterations > 0);