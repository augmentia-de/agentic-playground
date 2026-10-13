package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.event.TaskEvent;
import de.augmentia.agentplatform.common.model.CheckpointStatus;
import de.augmentia.agentplatform.common.model.ExecutionStatus;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowExecutionRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A long, deterministic workflow that <em>runs through completely</em>.
 *
 * <p>{@link ComplexWorkflowEngineTest} checks at waypoints: it starts the
 * most complex workflow and examines the state after every single
 * step. That is precise but does not answer the question of whether a workflow
 * with many steps and decisions <b>runs to the end at all</b>.
 * A routing bug that only shows in the sixth step stays there
 * unnoticed.
 *
 * <p>This test drives a deliberately simple but long DAG through to
 * {@code COMPLETED} and at the end checks the <em>entire</em> chain:
 *
 * <pre>
 *   s1 → g1 → s2 → g2 → s3 → g3 → s4 → g4 → s5 → end
 *        │         │         │         │
 *        └─────────┴─────────┴─────────┴─── every branch leads to skip*_a (SKIPPED)
 * </pre>
 *
 * <p>Five steps, four decisions, each with an active and a
 * discarded branch. Deliberately without LOOP, FORK and JOIN: the construction is meant to
 * test the <em>number</em> of the decisions, not additionally the types that
 * {@code ComplexWorkflowEngineTest} already covers.
 *
 * <p><b>Determinism.</b> the worker is simulated: the published
 * TaskEvent is answered with given fields. Every condition reads a
 * field the test sets itself — there is no LLM call, no
 * time dependency and no source of randomness. The test is thereby the same in every
 * environment and needs no key.
 *
 * <p>The engine is the real {@link WorkflowEngine} against real Postgres with
 * real transactions; only the Kafka publisher is recorded.
 */
@DisplayName("A long workflow runs through completely")
class LongWorkflowRunToCompletionTest extends WorkflowEngineTestBase {

    /**
     * Five steps, four decisions. Each decision has an active
     * branch (the condition matches) and a discarded one (it does not match).
     *
     * <p>The conditions use the flat context path
     * {@code context['sN'].flag}, because that is exactly the production contract —
     * siehe {@code ExpressionEvaluatorContextContractTest}.
     */
    private UUID longWorkflow() {
        return insertWorkflow("test-long-run", """
                {
                  "nodes": [
                    {"stepId":"s1","nodeType":"STEP","agentType":"researcher","dependsOn":[]},
                    {"stepId":"g1","nodeType":"DECISION","dependsOn":["s1"],
                     "branches":[{"branchId":"go1",
                       "condition":"context['s1'].flag == 'yes'","target":"s2"}],
                     "defaultTarget":"skip1"},
                    {"stepId":"s2","nodeType":"STEP","agentType":"coder","dependsOn":["g1"]},
                    {"stepId":"g2","nodeType":"DECISION","dependsOn":["s2"],
                     "branches":[{"branchId":"go2",
                       "condition":"context['s2'].flag == 'yes'","target":"s3"}],
                     "defaultTarget":"skip2"},
                    {"stepId":"s3","nodeType":"STEP","agentType":"tester","dependsOn":["g2"]},
                    {"stepId":"g3","nodeType":"DECISION","dependsOn":["s3"],
                     "branches":[{"branchId":"go3",
                       "condition":"context['s3'].flag == 'yes'","target":"s4"}],
                     "defaultTarget":"skip3"},
                    {"stepId":"s4","nodeType":"STEP","agentType":"reviewer","dependsOn":["g3"]},
                    {"stepId":"g4","nodeType":"DECISION","dependsOn":["s4"],
                     "branches":[{"branchId":"go4",
                       "condition":"context['s4'].flag == 'yes'","target":"s5"}],
                     "defaultTarget":"skip4"},
                    {"stepId":"s5","nodeType":"STEP","agentType":"test_writer","dependsOn":["g4"]},
                    {"stepId":"skip1","nodeType":"STEP","agentType":"coder","dependsOn":["g1"]},
                    {"stepId":"skip2","nodeType":"STEP","agentType":"coder","dependsOn":["g2"]},
                    {"stepId":"skip3","nodeType":"STEP","agentType":"coder","dependsOn":["g3"]},
                    {"stepId":"skip4","nodeType":"STEP","agentType":"coder","dependsOn":["g4"]},
                    {"stepId":"end","nodeType":"END","dependsOn":["s5","skip1","skip2","skip3","skip4"]}
                  ]
                }
                """);
    }

    /**
     * Plays one step through: answers the last task with
     * {@code flag=yes} and continues until there is nothing left to do.
     *
     * <p>Entscheidet der Workflow auf {@code defaultTarget}, endet die Schleife
     * not in an error but in an aborted run — the test
     * checks that explicitly afterwards. That is why the method returns how far
     * the workflow actually got.
     *
     * @return the order of the steps actually executed
     */
    private List<String> driveToEnd(UUID executionId, List<String> allSteps) {
        List<String> executed = new ArrayList<>();

        for (String step : allSteps) {
            List<TaskEvent> tasks = tasksFor(step);
            if (tasks.isEmpty()) {
                // this step was not reached — the chain ends here.
                break;
            }
            var task = tasks.get(tasks.size() - 1);
            engine.onResult(resultFor(task, Map.of(
                    "status", "COMPLETED",
                    "flag", "yes",
                    "confidence", 0.95d)));
            executed.add(step);
        }
        return executed;
    }

    @Test
    @DisplayName("Five steps and four decisions run through to COMPLETED")
    void longWorkflowReachesCompletion() {
        WorkflowExecutionRow execution = start(longWorkflow(), "{}");

        List<String> executed = driveToEnd(execution.id(),
                List.of("s1", "s2", "s3", "s4", "s5"));

        assertThat(executed)
                .as("Every step must actually have been dispatched — "
                    + "breaks the chain earlier, that shows up here")
                .containsExactly("s1", "s2", "s3", "s4", "s5");

        assertThat(status(execution.id()))
                .as("The workflow must be completed at the END node")
                .isEqualTo(ExecutionStatus.COMPLETED);
    }

    @Test
    @DisplayName("Every decision takes its active branch, not the default")
    void everyDecisionTakesItsBranch() {
        WorkflowExecutionRow execution = start(longWorkflow(), "{}");
        driveToEnd(execution.id(), List.of("s1", "s2", "s3", "s4", "s5"));

        // Without this check a workflow would also succeed if every
        // decision silently ran into the defaultTarget: the default leads
        // to an end too, the run would be COMPLETED — only entirely
        // wrong. Exactly that bug occurred in live operation.
        assertThat(checkpoint(execution.id(), "g1")).isPresent()
                .get().satisfies(c -> {
                    assertThat(c.branchId()).isEqualTo("go1");
                    assertThat(c.decisionBranch()).isEqualTo("s2");
                });
        assertThat(checkpoint(execution.id(), "g2")).isPresent()
                .get().satisfies(c -> {
                    assertThat(c.branchId()).isEqualTo("go2");
                    assertThat(c.decisionBranch()).isEqualTo("s3");
                });
        assertThat(checkpoint(execution.id(), "g3")).isPresent()
                .get().satisfies(c -> {
                    assertThat(c.branchId()).isEqualTo("go3");
                    assertThat(c.decisionBranch()).isEqualTo("s4");
                });
        assertThat(checkpoint(execution.id(), "g4")).isPresent()
                .get().satisfies(c -> {
                    assertThat(c.branchId()).isEqualTo("go4");
                    assertThat(c.decisionBranch()).isEqualTo("s5");
                });
    }

    @Test
    @DisplayName("The four discarded branches are SKIPPED, not PENDING")
    void discardedBranchesAreSkipped() {
        WorkflowExecutionRow execution = start(longWorkflow(), "{}");
        driveToEnd(execution.id(), List.of("s1", "s2", "s3", "s4", "s5"));

        for (String skipped : List.of("skip1", "skip2", "skip3", "skip4")) {
            assertThat(checkpoint(execution.id(), skipped))
                    .as("%s was never entered", skipped)
                    .isPresent()
                    .get()
                    .extracting(c -> c.status())
                    .isEqualTo(CheckpointStatus.SKIPPED);
        }
    }

    @Test
    @DisplayName("At the end no PENDING checkpoint exists any more")
    void noStepIsLeftPending() {
        WorkflowExecutionRow execution = start(longWorkflow(), "{}");
        driveToEnd(execution.id(), List.of("s1", "s2", "s3", "s4", "s5"));

        List<String> pending = jdbc.queryForList(
                "SELECT step_id FROM workflow_checkpoints "
                        + "WHERE workflow_execution_id = ? AND status = 'PENDING'",
                String.class, execution.id());

        assertThat(pending)
                .as("A lingering PENDING checkpoint would make the sweep reaper "
                    + "fire later — here none may remain")
                .isEmpty();
    }

    @Test
    @DisplayName("If a decision aborts, the workflow still ends cleanly")
    void abortingDecisionStillCompletes() {
        // s1 liefert flag=no -> g1 nimmt skip1. Alles Weitere ist unerreichbar
        // and is cascaded. The run must nevertheless become COMPLETED: an
        // aborted workflow is a valid result, not an error.
        WorkflowExecutionRow execution = start(longWorkflow(), "{}");

        var task = tasksFor("s1").get(0);
        engine.onResult(resultFor(task, Map.of(
                "status", "COMPLETED",
                "flag", "no",
                "confidence", 0.2d)));

        assertThat(checkpoint(execution.id(), "g1")).isPresent()
                .get().satisfies(c -> {
                    assertThat(c.branchId())
                            .as("Without a match on 'go1' the defaultTarget applies")
                            .isEqualTo("default");
                    assertThat(c.decisionBranch()).isEqualTo("skip1");
                });

        // g1 pointed at skip1 — skip1 is a completely normal step and
        // laeuft ebenfalls. Erst wenn er beantwortet ist, kann END greifen.
        assertThat(tasksFor("skip1"))
                .as("The defaultTarget is a regular step, not an abort")
                .hasSize(1);
        var skip = tasksFor("skip1").get(0);
        engine.onResult(resultFor(skip, Map.of("status", "COMPLETED", "flag", "no")));

        assertThat(status(execution.id()))
                .as("The aborted path is a regular end")
                .isEqualTo(ExecutionStatus.COMPLETED);
    }

    @Test
    @DisplayName("Every step publishes exactly one task on a task topic")
    void eachStepPublishesExactlyOneTask() {
        WorkflowExecutionRow execution = start(longWorkflow(), "{}");
        driveToEnd(execution.id(), List.of("s1", "s2", "s3", "s4", "s5"));

        assertThat(dispatchedTasks())
                .as("Five steps, one task each — more would mean double dispatch, "
                    + "fewer a lost step")
                .hasSize(5)
                .extracting(t -> t.stepId())
                .containsExactly("s1", "s2", "s3", "s4", "s5");
    }
}
