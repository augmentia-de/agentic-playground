-- Tenant isolation: add tenant_id column to core tables
CREATE TABLE IF NOT EXISTS tenants (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    billing_plan VARCHAR(64),
    expires_at TIMESTAMP WITH TIME ZONE,
    max_workflows INT,
    monthly_quota_usd NUMERIC(12, 2),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE workflow_executions ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) NOT NULL DEFAULT 'default-tenant';
ALTER TABLE workflow_checkpoints ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) NOT NULL DEFAULT 'default-tenant';
ALTER TABLE hitl_tickets ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64) NOT NULL DEFAULT 'default-tenant';

CREATE INDEX IF NOT EXISTS idx_workflow_executions_tenant ON workflow_executions(tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_checkpoints_tenant ON workflow_checkpoints(tenant_id, workflow_execution_id);
CREATE INDEX IF NOT EXISTS idx_hitl_tickets_tenant ON hitl_tickets(tenant_id, status);

-- Seed default-tenant record
INSERT INTO tenants (id, name, active) VALUES ('default-tenant', 'Default Tenant', true)
ON CONFLICT (id) DO NOTHING;
