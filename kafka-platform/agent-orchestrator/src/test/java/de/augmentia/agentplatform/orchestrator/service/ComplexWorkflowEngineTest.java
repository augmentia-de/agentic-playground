package de.augmentia.agentplatform.orchestrator.service;

import de.augmentia.agentplatform.common.event.ControlEvents;
import de.augmentia.agentplatform.common.event.TaskEvent;
import de.augmentia.agentplatform.common.model.CheckpointStatus;
import de.augmentia.agentplatform.common.model.ExecutionStatus;
import de.augmentia.agentplatform.common.model.HitlAction;
import de.augmentia.agentplatform.orchestrator.domain.WorkflowExecutionRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The most complex shipped workflow through the <em>real</em> engine.
 *
 * <p>{@code reference-code-quality} covers every node type the platform
 * knows:
 *
 * <pre>
 *   research_step (STEP)
 *        │
 *   quality_gate (DECISION) ─── confidence &lt; 0.8 ──→ refinement_loop (LOOP, max 3)
 *        │                                                    │ exitCondition
 *        └── defaultTarget ────────────────────────────────→ parallel_fork (FORK)
 *                                                                ├─ b.code.gen  (STEP)
 *                                                                └─ b.test.prep (STEP)
 *                                                                      │
 *                                                              merge_gate (JOIN, AND)
 *                                                                      │
 *                                                        final_review (STEP + HITL-Gate)
 *                                                                      │
 *                                                                    end (END)
 * </pre>
 *
 * <p>The difference to {@code WorkflowRoutingScenarioTest}: there
 * {@code RoutingEvaluator.evaluate()} is called directly with a hand-built
 * checkpoint simulation. Here the same definition runs through
 * {@link WorkflowEngine} with real transactions, real checkpoint rows and
 * real progress persistence. This is the first time it is covered that LOOP,
 * FORK, JOIN and the HITL gate <em>inside the engine</em> correctly
 * play together — previously only the evaluator was tested, not the engine.
 *
 * <p>The worker is simulated: the TaskEvent published by the orchestrator is
 * looked up and answered with a self-built {@code ResultEvent}.
 * The path engine → routing → TaskEvent is real.
 */
@DisplayName("Complex workflow through the engine")
class ComplexWorkflowEngineTest extends WorkflowEngineTestBase {

    /** Baut reference-code-quality in der Testdatenbank an. */
    private UUID referenceCodeQuality() {
        return insertWorkflow("test-reference-code-quality", """
                {
                  "nodes": [
                    {"stepId":"research_step","nodeType":"STEP","agentType":"researcher","dependsOn":[]},
                    {"stepId":"quality_gate","nodeType":"DECISION","dependsOn":["research_step"],
                     "branches":[
                       {"branchId":"refine","condition":"context['research_step'].confidence < 0.8",
                        "target":"refinement_loop"}
                     ],
                     "defaultTarget":"parallel_fork"},
                    {"stepId":"refinement_loop","nodeType":"LOOP","bodyAgentType":"refiner",
                     "maxIterations":3,
                     "exitCondition":"context['refinement_loop'].confidence >= 0.8",
                     "dependsOn":["quality_gate"]},
                    {"stepId":"parallel_fork","nodeType":"FORK",
                     "dependsOn":["quality_gate","refinement_loop"],
                     "branches":[
                       {"branchId":"code","firstStep":"b.code.gen"},
                       {"branchId":"test","firstStep":"b.test.prep"}
                     ]},
                    {"stepId":"b.code.gen","nodeType":"STEP","agentType":"coder","dependsOn":["parallel_fork"]},
                    {"stepId":"b.test.prep","nodeType":"STEP","agentType":"test_writer","dependsOn":["parallel_fork"]},
                    {"stepId":"merge_gate","nodeType":"JOIN","joinType":"AND",
                     "joinBranches":["code","test"],
                     "dependsOn":["b.code.gen","b.test.prep"]},
                    {"stepId":"final_review","nodeType":"STEP","agentType":"reviewer",
                     "requiresHumanApproval":true,"dependsOn":["merge_gate"]},
                    {"stepId":"end","nodeType":"END","dependsOn":["final_review"]}
                  ]
                }
                """);
    }

    // ── Der Default-Pfad: DECISION → defaultTarget → FORK → JOIN → HITL → END ──

    @Test
    @DisplayName("High confidence: DECISION takes defaultTarget, FORK fires both branches")
    void highConfidenceTakesDefaultTargetAndForks() {
        WorkflowExecutionRow execution = start(referenceCodeQuality(), "{}");

        // Step 1: research_step yields high confidence -> refine does NOT match
        completeStep("research_step", "COMPLETED", Map.of("confidence", 0.92d, "findings", "3"));

        assertThat(checkpoint(execution.id(), "quality_gate")).isPresent()
                .get().extracting(c -> c.branchId())
                .as("Without a match on 'refine' the defaultTarget = parallel_fork applies")
                .isEqualTo("default");
        assertThat(checkpoint(execution.id(), "quality_gate")).isPresent()
                .get().extracting(c -> c.decisionBranch())
                .isEqualTo("parallel_fork");

        // FORK fires both branches in parallel
        assertThat(tasksFor("b.code.gen")).as("code-Zweig").hasSize(1);
        assertThat(tasksFor("b.test.prep")).as("test-Zweig").hasSize(1);
        assertThat(tasksFor("refinement_loop"))
                .as("The LOOP was not entered")
                .isEmpty();

        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.RUNNING);
    }

    @Test
    @DisplayName("JOIN(AND) waits until both branches are done")
    void joinWaitsForBothBranches() {
        WorkflowExecutionRow execution = start(referenceCodeQuality(), "{}");
        completeStep("research_step", "COMPLETED", Map.of("confidence", 0.92d));

        // only the code branch answers -> the JOIN must not fire yet
        completeStep("b.code.gen", "COMPLETED", Map.of("artifact", "code"));
        assertThat(tasksFor("final_review"))
                .as("JOIN(AND) must not proceed with a half-done FORK")
                .isEmpty();
        assertThat(checkpoint(execution.id(), "b.test.prep")).isPresent()
                .get().extracting(c -> c.status()).isEqualTo(CheckpointStatus.IN_PROGRESS);

        // the second branch answers -> now the JOIN may fire. The JOIN is a
        // pure control node; afterwards final_review waits for a release, so
        // no task is published, a ticket is created instead.
        completeStep("b.test.prep", "COMPLETED", Map.of("artifact", "tests"));

        assertThat(checkpoint(execution.id(), "merge_gate")).isPresent()
                .get().extracting(c -> c.status())
                .as("JOIN(AND) is fulfilled as soon as both branches are done")
                .isEqualTo(CheckpointStatus.COMPLETED);
        assertThat(checkpoint(execution.id(), "final_review")).isPresent()
                .get().extracting(c -> c.status())
                .isEqualTo(CheckpointStatus.WAITING_HITL);
        assertThat(ticketStore.findOpenByExecution(execution.id()))
                .isNotEmpty()
                .allSatisfy(t -> assertThat(t.stepId()).isEqualTo("final_review"));
    }

    @Test
    @DisplayName("The HITL gate before final_review pauses and continues after approval")
    void approvalGatePausesAndResumes() {
        WorkflowExecutionRow execution = start(referenceCodeQuality(), "{}");
        completeStep("research_step", "COMPLETED", Map.of("confidence", 0.92d));
        completeStep("b.code.gen", "COMPLETED", Map.of("artifact", "code"));
        completeStep("b.test.prep", "COMPLETED", Map.of("artifact", "tests"));

        // final_review requires approval -> no task, ticket, PAUSED_HITL
        assertThat(tasksFor("final_review"))
                .as("Without approval final_review must not run")
                .isEmpty();
        assertThat(checkpoint(execution.id(), "final_review")).isPresent()
                .get().extracting(c -> c.status()).isEqualTo(CheckpointStatus.WAITING_HITL);
        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.PAUSED_HITL);
        var ticket = ticketStore.findOpenByExecution(execution.id()).get(0);
        assertThat(ticket.stepId()).isEqualTo("final_review");
        assertThat(ticket.reason()).isEqualTo("APPROVAL_REQUIRED");

        // Freigabe
        engine.onHitl(new ControlEvents.HitlEvent(
                execution.id().toString(), ticket.id().toString(), ticket.stepId(),
                ticket.iteration(), HitlAction.APPROVED, "operator"));

        assertThat(tasksFor("final_review")).as("After the release the step runs").hasSize(1);
        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.RUNNING);
    }

    @Test
    @DisplayName("Complete run through to COMPLETED including HITL")
    void fullRunReachesCompleted() {
        WorkflowExecutionRow execution = start(referenceCodeQuality(), "{}");

        completeStep("research_step", "COMPLETED", Map.of("confidence", 0.92d));
        completeStep("b.code.gen", "COMPLETED", Map.of("artifact", "code"));
        completeStep("b.test.prep", "COMPLETED", Map.of("artifact", "tests"));

        var ticket = ticketStore.findOpenByExecution(execution.id()).get(0);
        engine.onHitl(new ControlEvents.HitlEvent(
                execution.id().toString(), ticket.id().toString(), ticket.stepId(),
                ticket.iteration(), HitlAction.APPROVED, "operator"));
        completeStep("final_review", "COMPLETED", Map.of("approved", true));

        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.COMPLETED);

        // the branch not taken (refinement_loop) is SKIPPED by definition —
        // that is exactly how the evaluator marks DECISION subtrees that were not chosen. All
        // steps that actually ran must be COMPLETED.
        assertThat(engine.findCheckpoints(execution.id()))
                .filteredOn(c -> !"refinement_loop".equals(c.stepId()))
                .isNotEmpty()
                .allSatisfy(c -> assertThat(c.status())
                        .as("Step %s", c.stepId())
                        .isEqualTo(CheckpointStatus.COMPLETED));
        assertThat(checkpoint(execution.id(), "refinement_loop")).isPresent()
                .get().extracting(c -> c.status())
                .as("The LOOP branch not taken is skipped, not executed")
                .isEqualTo(CheckpointStatus.SKIPPED);
    }

    // ── Der LOOP-Pfad ────────────────────────────────────────────────

    @Test
    @DisplayName("Low confidence: DECISION takes 'refine' and fires the LOOP")
    void lowConfidenceEntersRefinementLoop() {
        WorkflowExecutionRow execution = start(referenceCodeQuality(), "{}");

        completeStep("research_step", "COMPLETED", Map.of("confidence", 0.5d));

        assertThat(checkpoint(execution.id(), "quality_gate")).isPresent()
                .get().extracting(c -> c.branchId())
                .as("Match on 'refine' -> branch ID is persisted")
                .isEqualTo("refine");
        assertThat(tasksFor("refinement_loop")).as("Der LOOP feuert").hasSize(1);
        assertThat(tasksFor("b.code.gen"))
                .as("The fork may only run after the LOOP")
                .isEmpty();
    }

    @Test
    @DisplayName("LOOP: a result iteration without exit condition yields the next one")
    void loopIteratesUntilExitConditionHolds() {
        WorkflowExecutionRow execution = start(referenceCodeQuality(), "{}");
        completeStep("research_step", "COMPLETED", Map.of("confidence", 0.5d));

        TaskEvent first = lastDispatchedTask();
        assertThat(first.stepId()).isEqualTo("refinement_loop");
        assertThat(first.iteration()).isEqualTo(1);

        // result of the LOOP node with confidence < 0.8 -> exitCondition not satisfied
        engine.onResult(resultFor(first, Map.of(
                "status", "COMPLETED", "confidence", 0.6d)));
        engine.onLoop(new ControlEvents.LoopEvent(
                execution.id().toString(), "refinement_loop", 1, false, 3, Map.of("confidence", 0.6d)));

        // Zweite Iteration
        assertThat(tasksFor("refinement_loop")).hasSize(2);
        TaskEvent second = lastDispatchedTask();
        assertThat(second.iteration()).as("Iteration inkrementiert").isEqualTo(2);
        assertThat(second.header().attempt()).isEqualTo(1);
    }

    @Test
    @DisplayName("LOOP: a satisfied exit condition leads on to the fork")
    void loopExitContinuesToFork() {
        WorkflowExecutionRow execution = start(referenceCodeQuality(), "{}");
        completeStep("research_step", "COMPLETED", Map.of("confidence", 0.5d));

        TaskEvent first = lastDispatchedTask();
        engine.onResult(resultFor(first, Map.of("status", "COMPLETED", "confidence", 0.9d)));
        engine.onLoop(new ControlEvents.LoopEvent(
                execution.id().toString(), "refinement_loop", 1, true, 3, Map.of("confidence", 0.9d)));

        assertThat(checkpoint(execution.id(), "refinement_loop")).isPresent()
                .get().extracting(c -> c.status()).isEqualTo(CheckpointStatus.COMPLETED);
        assertThat(tasksFor("b.code.gen")).as("After the LOOP the fork fires").hasSize(1);
        assertThat(tasksFor("b.test.prep")).hasSize(1);
    }

    @Test
    @DisplayName("LOOP: exhausted iterations create a HITL ticket instead of an endless loop")
    void loopExhaustionCreatesHitlTicket() {
        WorkflowExecutionRow execution = start(referenceCodeQuality(), "{}");
        completeStep("research_step", "COMPLETED", Map.of("confidence", 0.5d));

        // three iterations without an exit condition
        for (int iteration = 1; iteration <= 3; iteration++) {
            TaskEvent loop = lastDispatchedTask();
            assertThat(loop.iteration()).isEqualTo(iteration);
            engine.onResult(resultFor(loop, Map.of("status", "COMPLETED", "confidence", 0.1d)));
            engine.onLoop(new ControlEvents.LoopEvent(
                    execution.id().toString(), "refinement_loop", iteration, false, 3,
                    Map.of("confidence", 0.1d)));
        }

        assertThat(status(execution.id())).isEqualTo(ExecutionStatus.PAUSED_HITL);
        assertThat(ticketStore.findOpenByExecution(execution.id()))
                .isNotEmpty()
                .allSatisfy(t -> assertThat(t.reason()).isEqualTo("LOOP_MAX_ITERATIONS"));
        assertThat(tasksFor("b.code.gen"))
                .as("After the iteration limit the fork must not proceed")
                .isEmpty();
    }

    // ── Idempotenz auf komplexer Ebene ───────────────────────────────

    @Test
    @DisplayName("A duplicate answer to the same task creates no second progress")
    void duplicateResultDoesNotAdvanceTwice() {
        WorkflowExecutionRow execution = start(referenceCodeQuality(), "{}");
        TaskEvent research = completeStep("research_step", "COMPLETED", Map.of("confidence", 0.92d));

        int forksAfterFirst = tasksFor("b.code.gen").size();
        assertThat(forksAfterFirst).isEqualTo(1);

        // the same result once more
        engine.onResult(resultFor(research, Map.of("confidence", 0.92d)));

        assertThat(tasksFor("b.code.gen"))
                .as("A doubly delivered result must not trigger a second FORK")
                .hasSize(forksAfterFirst);
    }
}
