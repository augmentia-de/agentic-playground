/* Shedlock — shared lock table for orchestrator lease (JDBC).
 * Only one node at a time can hold the same `lock_name`. */
CREATE TABLE IF NOT EXISTS shedlock (
    name          VARCHAR(64) PRIMARY KEY,
    lock_until    TIMESTAMP WITH TIME ZONE NOT NULL,
    locked_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    locked_by     VARCHAR(256) NOT NULL,
    CONSTRAINT chk_lock_until CHECK (lock_until > locked_at)
);
