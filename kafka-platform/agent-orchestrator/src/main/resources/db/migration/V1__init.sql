CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE agent_definitions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    agent_type VARCHAR(64) NOT NULL UNIQUE,
    description TEXT,
    system_prompt TEXT NOT NULL,
    model_name VARCHAR(64) NOT NULL DEFAULT 'mock-deterministic',
    temperature NUMERIC(3, 2) NOT NULL DEFAULT 0.00,
    max_tokens INT NOT NULL DEFAULT 4096,
    tools_config JSONB NOT NULL DEFAULT '[]'::jsonb,
    timeout_seconds INT NOT NULL DEFAULT 60,
    max_retry_attempts INT NOT NULL DEFAULT 3,
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE workflow_definitions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(128) NOT NULL,
    version INT NOT NULL DEFAULT 1,
    dag_structure JSONB NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_workflow_name_version UNIQUE (name, version)
);

CREATE TABLE workflow_executions (
    id UUID PRIMARY KEY,
    workflow_definition_id UUID NOT NULL REFERENCES workflow_definitions(id),
    idempotency_key VARCHAR(255),
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    input_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    final_output JSONB,
    error_details JSONB,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_workflow_execution_idempotency_key UNIQUE (idempotency_key)
);

CREATE TABLE workflow_checkpoints (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workflow_execution_id UUID NOT NULL REFERENCES workflow_executions(id) ON DELETE CASCADE,
    step_id VARCHAR(128) NOT NULL,
    iteration INT NOT NULL DEFAULT 1,
    node_type VARCHAR(32) NOT NULL DEFAULT 'STEP',
    agent_type VARCHAR(64),
    task_id UUID,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    decision_branch VARCHAR(128),
    input_payload JSONB,
    output_payload JSONB,
    retry_count INT NOT NULL DEFAULT 0,
    attempt INT NOT NULL DEFAULT 1,
    max_attempts INT NOT NULL DEFAULT 3,
    timeout_ms BIGINT NOT NULL DEFAULT 60000,
    error_message TEXT,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_execution_step_iteration UNIQUE (workflow_execution_id, step_id, iteration)
);

CREATE TABLE hitl_tickets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workflow_execution_id UUID NOT NULL REFERENCES workflow_executions(id) ON DELETE CASCADE,
    checkpoint_id UUID REFERENCES workflow_checkpoints(id) ON DELETE SET NULL,
    step_id VARCHAR(128) NOT NULL,
    iteration INT NOT NULL DEFAULT 1,
    reason VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'OPEN',
    assigned_role VARCHAR(64) NOT NULL DEFAULT 'OPERATOR',
    context_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    resolution_notes TEXT,
    resolved_by VARCHAR(128),
    escalation_count INT NOT NULL DEFAULT 0,
    expires_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE dlq_messages (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    topic VARCHAR(128) NOT NULL,
    original_key VARCHAR(256),
    payload JSONB NOT NULL,
    error_class VARCHAR(256),
    error_message TEXT,
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 5,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    resolved_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_agent_definitions_type ON agent_definitions(agent_type);
CREATE INDEX idx_workflow_executions_status ON workflow_executions(status);
CREATE INDEX idx_workflow_executions_def_id ON workflow_executions(workflow_definition_id);
CREATE INDEX idx_workflow_executions_updated_at ON workflow_executions(updated_at);
CREATE INDEX idx_checkpoints_execution_id ON workflow_checkpoints(workflow_execution_id);
CREATE INDEX idx_checkpoints_task_id ON workflow_checkpoints(task_id);
CREATE INDEX idx_checkpoints_status ON workflow_checkpoints(status);
CREATE INDEX idx_checkpoints_stale ON workflow_checkpoints(status, started_at)
    WHERE status IN ('IN_PROGRESS', 'PENDING');
CREATE INDEX idx_hitl_tickets_status ON hitl_tickets(status);
CREATE INDEX idx_hitl_tickets_workflow ON hitl_tickets(workflow_execution_id);
CREATE INDEX idx_hitl_tickets_expires ON hitl_tickets(expires_at) WHERE status = 'OPEN';
CREATE INDEX idx_dlq_status ON dlq_messages(status);
CREATE INDEX idx_dlq_next_retry ON dlq_messages(next_retry_at) WHERE status = 'PENDING';
