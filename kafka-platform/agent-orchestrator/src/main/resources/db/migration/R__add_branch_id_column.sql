/* Separates the branch ID from the target step.
 *
 * Previously `decision_branch` stored the *target step* of a DECISION selection.
 * The actual branch ID (the name of the branch) was lost in the process and was
 * only reconstructible via the naming convention "b.<branchId>." of the branch label.
 * Without it the persistence cannot answer
 * which branch ran — relevant for analytics and incident post-mortems.
 *
 * `decision_branch` remains unchanged (target step) so that existing evaluations
 * keep working; the branch ID goes into the new column. */
ALTER TABLE workflow_checkpoints
    ADD COLUMN IF NOT EXISTS branch_id VARCHAR(128);

COMMENT ON COLUMN workflow_checkpoints.branch_id IS
    'Chosen branch ID of a DECISION selection; "default" for defaultTarget, NULL for other nodes';
