package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.model.CheckpointStatus;
import de.augmentia.agentplatform.common.model.ExecutionStatus;
import de.augmentia.agentplatform.common.event.TaskEvent;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowExecutionRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Region loop through the <em>real</em> engine.
 *
 * <p>In contrast to the cooperative single-agent loop, here a whole region of
 * several agents is repeated. The LOOP node is routing-only (it is never dispatched to a
 * worker); the body steps {@code a1..a5} run sequentially per iteration, and
 * the iteration is derived from the persisted body checkpoints. Thus the
 * path definition → validation → store load → routing → dispatch → checkpoint is real.
 */
@DisplayName("Region loop through the engine")
class RegionLoopEngineTest extends WorkflowEngineTestBase {

    /** start → loop(body a1..a5, max 3) → after → end. */
    private UUID regionLoopWorkflow() {
        return insertWorkflow("test-region-loop", """
                {
                  "nodes": [
                    {"stepId":"start","nodeType":"STEP","agentType":"researcher","dependsOn":[]},
                    {"stepId":"loop","nodeType":"LOOP","maxIterations":3,
                     "exitCondition":"context['a5'].done == true",
                     "bodySteps":["a1","a2","a3","a4","a5"],
                     "dependsOn":["start"]},
                    {"stepId":"a1","nodeType":"STEP","agentType":"coder","dependsOn":["loop"]},
                    {"stepId":"a2","nodeType":"STEP","agentType":"coder","dependsOn":["a1"]},
                    {"stepId":"a3","nodeType":"STEP","agentType":"coder","dependsOn":["a2"]},
                    {"stepId":"a4","nodeType":"STEP","agentType":"coder","dependsOn":["a3"]},
                    {"stepId":"a5","nodeType":"STEP","agentType":"coder","dependsOn":["a4"]},
                    {"stepId":"after","nodeType":"STEP","agentType":"reviewer","dependsOn":["a5"]},
                    {"stepId":"end","nodeType":"END","dependsOn":["after"]}
                  ]
                }
                """);
    }

    /** Runs the body of one iteration through to the end; yields the last task. */
    private TaskEvent completeBodyOnce(UUID executionId, boolean done) {
        assertThat(tasksFor("a1")).isNotEmpty();
        completeStep("a1", "COMPLETED", Map.of());
        completeStep("a2", "COMPLETED", Map.of());
        completeStep("a3", "COMPLETED", Map.of());
        completeStep("a4", "COMPLETED", Map.of());
        return completeStep("a5", "COMPLETED", Map.of("done", done));
    }

    @Test
    @DisplayName("Der LOOP ist routing-only; erst der Body-Einstieg a1 feuert")
    void regionLoopFiresBodyEntryOnly() {
        WorkflowExecutionRow execution = start(regionLoopWorkflow(), "{}");

        assertThat(tasksFor("loop"))
                .as("A region LOOP is never dispatched to a worker")
                .isEmpty();
        assertThat(tasksFor("a1")).as("a1 wartet auf start").isEmpty();

        completeStep("start", "COMPLETED", Map.of());

        assertThat(tasksFor("a1")).hasSize(1);
        assertThat(lastDispatchedTask().iteration()).isEqualTo(1);
        assertThat(tasksFor("a2")).as("Body-Step-Kette ist sequenziell").isEmpty();
        assertThat(checkpoint(execution.id(), "a1")).isPresent()
                .get().extracting(c -> c.status()).isEqualTo(CheckpointStatus.IN_PROGRESS);
        assertThat(checkpoint(execution.id(), "loop")).isPresent()
                .get().satisfies(c -> {
                    assertThat(c.status()).isEqualTo(CheckpointStatus.IN_PROGRESS);
                    assertThat(c.iteration()).as("Loop laeuft in Iteration 1").isEqualTo(1);
                });
    }

    @Test
    @DisplayName("Without exit the loop re-arms: the body starts again in iteration 2")
    void regionLoopRearmsNextIteration() {
        WorkflowExecutionRow execution = start(regionLoopWorkflow(), "{}");
        completeStep("start", "COMPLETED", Map.of());

        assertThat(tasksFor("a1")).hasSize(1);
        completeBodyOnce(execution.id(), false);

        assertThat(tasksFor("a1")).as("Body-Einstieg feuert erneut").hasSize(2);
        assertThat(lastDispatchedTask().stepId()).isEqualTo("a1");
        assertThat(lastDispatchedTask().iteration()).as("Iteration inkrementiert").isEqualTo(2);
        assertThat(checkpoint(execution.id(), "loop")).isPresent()
                .get().satisfies(c -> {
                    assertThat(c.status()).isEqualTo(CheckpointStatus.IN_PROGRESS);
                    assertThat(c.iteration()).as("Loop-Checkpoint folgt der Iteration").isEqualTo(2);
                });
        assertThat(tasksFor("after")).as("Downstream waits for the loop end").isEmpty();
    }

    @Test
    @DisplayName("The exit condition ends the loop and releases the downstream")
    void regionLoopExitReleasesDownstream() {
        WorkflowExecutionRow execution = start(regionLoopWorkflow(), "{}");
        completeStep("start", "COMPLETED", Map.of());

        completeBodyOnce(execution.id(), true);

        assertThat(checkpoint(execution.id(), "loop")).isPresent()
                .get().extracting(c -> c.status()).isEqualTo(CheckpointStatus.COMPLETED);
        assertThat(tasksFor("after")).as("after fires after the loop").hasSize(1);
        assertThat(tasksFor("a1")).as("no re-arm after exit").hasSize(1);
    }

    @Test
    @DisplayName("maxIterations stops the loop and releases the downstream")
    void regionLoopStopsAtMaxIterations() {
        WorkflowExecutionRow execution = start(regionLoopWorkflow(), "{}");
        completeStep("start", "COMPLETED", Map.of());

        for (int iteration = 1; iteration <= 3; iteration++) {
            assertThat(lastDispatchedTask().stepId()).isEqualTo("a1");
            assertThat(lastDispatchedTask().iteration()).isEqualTo(iteration);
            completeBodyOnce(execution.id(), false);
        }

        assertThat(checkpoint(execution.id(), "loop")).isPresent()
                .get().extracting(c -> c.status()).isEqualTo(CheckpointStatus.COMPLETED);
        assertThat(tasksFor("after")).hasSize(1);
        assertThat(tasksFor("a1")).as("no re-arm after the iteration limit").hasSize(3);
    }

    @Test
    @DisplayName("Complete run through to COMPLETED over two iterations")
    void regionLoopFullRunReachesCompleted() {
        WorkflowExecutionRow execution = start(regionLoopWorkflow(), "{}");
        completeStep("start", "COMPLETED", Map.of());

        completeBodyOnce(execution.id(), false);
        completeBodyOnce(execution.id(), true);

        assertThat(tasksFor("a1")).hasSize(2);
        completeStep("after", "COMPLETED", Map.of("ok", true));

        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.COMPLETED);
    }

    /* ---------------- control nodes inside the loop body ------------------ */

    /**
     * start → loop(body: FORK → zwei Branches → JOIN) → after → end.
     *
     * <p>The body thereby contains control nodes, not only STEPs. FORK and JOIN are
     * routing-only and get their own checkpoint per iteration.
     */
    private UUID regionLoopWithForkWorkflow() {
        return insertWorkflow("test-region-loop-fork", """
                {
                  "nodes": [
                    {"stepId":"start","nodeType":"STEP","agentType":"researcher","dependsOn":[]},
                    {"stepId":"loop","nodeType":"LOOP","maxIterations":2,
                     "exitCondition":"context['b.hit.hit_step'].done == true",
                     "bodySteps":["fk","b.hit.hit_step","b.miss.miss_step","j"],
                     "dependsOn":["start"]},
                    {"stepId":"fk","nodeType":"FORK","dependsOn":["loop"],
                     "branches":[
                       {"branchId":"hit","firstStep":"b.hit.hit_step"},
                       {"branchId":"miss","firstStep":"b.miss.miss_step"}
                     ]},
                    {"stepId":"b.hit.hit_step","nodeType":"STEP","agentType":"coder","dependsOn":["fk"]},
                    {"stepId":"b.miss.miss_step","nodeType":"STEP","agentType":"reviewer","dependsOn":["fk"]},
                    {"stepId":"j","nodeType":"JOIN","joinType":"AND",
                     "dependsOn":["b.hit.hit_step","b.miss.miss_step"]},
                    {"stepId":"after","nodeType":"STEP","agentType":"reviewer","dependsOn":["loop"]},
                    {"stepId":"end","nodeType":"END","dependsOn":["after"]}
                  ]
                }
                """);
    }

    /** Beantwortet beide Branches; der JOIN schliesst dadurch die Iteration ab. */
    private void runForkBodyIteration() {
        assertThat(tasksFor("b.hit.hit_step")).isNotEmpty();
        assertThat(tasksFor("b.miss.miss_step")).isNotEmpty();
        completeStep("b.hit.hit_step", "COMPLETED", Map.of());
        completeStep("b.miss.miss_step", "COMPLETED", Map.of());
    }

    private int checkpointRows(UUID executionId, String stepId) {
        return jdbc.queryForObject(
                "SELECT count(*) FROM workflow_checkpoints WHERE workflow_execution_id = ? AND step_id = ?",
                Integer.class, executionId, stepId);
    }

    @Test
    @DisplayName("FORK in the loop body dispatches both branches and closes routing-only")
    void forkInRegionBodyDispatchesBothBranches() {
        WorkflowExecutionRow execution = start(regionLoopWithForkWorkflow(), "{}");

        assertThat(tasksFor("fk")).as("FORK ist routing-only").isEmpty();
        completeStep("start", "COMPLETED", Map.of());

        assertThat(tasksFor("b.hit.hit_step")).hasSize(1);
        assertThat(tasksFor("b.miss.miss_step")).hasSize(1);
        assertThat(lastDispatchedTask().iteration()).isEqualTo(1);
        assertThat(checkpoint(execution.id(), "fk")).isPresent()
                .get().satisfies(c -> {
                    assertThat(c.status()).isEqualTo(CheckpointStatus.COMPLETED);
                    assertThat(c.iteration()).isEqualTo(1);
                });
    }

    @Test
    @DisplayName("FORK/JOIN in the loop body get their own checkpoint per iteration")
    void controlNodesInRegionBodyPersistPerIteration() {
        WorkflowExecutionRow execution = start(regionLoopWithForkWorkflow(), "{}");
        completeStep("start", "COMPLETED", Map.of());

        runForkBodyIteration();

        assertThat(tasksFor("b.hit.hit_step")).as("Branches feuern erneut").hasSize(2);
        assertThat(lastDispatchedTask().iteration()).isEqualTo(2);
        assertThat(checkpoint(execution.id(), "j")).isPresent()
                .get().satisfies(c -> {
                    assertThat(c.status()).isEqualTo(CheckpointStatus.COMPLETED);
                    assertThat(c.iteration()).as("JOIN-Completion der Iteration 1").isEqualTo(1);
                });
        assertThat(checkpointRows(execution.id(), "fk"))
                .as("FORK has one row per iteration").isEqualTo(2);
        assertThat(checkpoint(execution.id(), "loop")).isPresent()
                .get().satisfies(c -> {
                    assertThat(c.status()).isEqualTo(CheckpointStatus.IN_PROGRESS);
                    assertThat(c.iteration()).isEqualTo(2);
                });
    }

    @Test
    @DisplayName("A stale JOIN from the previous iteration does not hang the region")
    void staleJoinDoesNotStallRegion() {
        WorkflowExecutionRow execution = start(regionLoopWithForkWorkflow(), "{}");
        completeStep("start", "COMPLETED", Map.of());
        runForkBodyIteration();

        // Complete iteration 2: the JOIN still carries its iteration-1 row.
        runForkBodyIteration();

        assertThat(checkpoint(execution.id(), "j")).isPresent()
                .get().satisfies(c -> {
                    assertThat(c.status()).isEqualTo(CheckpointStatus.COMPLETED);
                    assertThat(c.iteration()).as("JOIN in Iteration 2 neu abgeschlossen").isEqualTo(2);
                });
        assertThat(checkpoint(execution.id(), "loop")).isPresent()
                .get().satisfies(c -> {
                    assertThat(c.status()).isEqualTo(CheckpointStatus.COMPLETED);
                    assertThat(c.iteration()).isEqualTo(2);
                });
        assertThat(tasksFor("after")).as("Downstream is released only after the loop end").hasSize(1);

        completeStep("after", "COMPLETED", Map.of("ok", true));
        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.COMPLETED);
    }
}
