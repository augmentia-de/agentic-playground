package de.augmentia.agentflow.app.activities;

import de.augmentia.agentflow.app.workflow.SagaActivities;
import de.augmentia.quad.core.agent.runtime.SagaAgentInterceptor;
import de.augmentia.quad.core.session.AgentSessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import io.temporal.spring.boot.ActivityImpl;

/** Side-effecting saga steps (would call real inventory/payment systems in production). */
@Component
@ActivityImpl(taskQueues = "agentflow")
public class SagaActivitiesImpl implements SagaActivities {

    private static final Logger log = LoggerFactory.getLogger(SagaActivitiesImpl.class);

    @Override
    public String reserve(String orderId) {
        log.info("[saga] reserve order={}", orderId);
        return "res-" + orderId;
    }

    @Override
    public String charge(String orderId, double amount) {
        if (amount > 100.0) {
            throw new IllegalStateException("payment declined for order " + orderId);
        }
        log.info("[saga] charge order={} amount={}", orderId, amount);
        return "charged:" + orderId;
    }

    @Override
    public void release(String reservationId) {
        log.warn("[saga] COMPENSATE release reservation={}", reservationId);
    }

    @Override
    public void refund(String orderId) {
        log.warn("[saga] COMPENSATE refund order={}", orderId);
    }

    @Override
    public String quadSaga(String input) {
        // Reuse of quad-core's session-level saga (compensation stack + unwind).
        AgentSessionState state = new AgentSessionState();
        SagaAgentInterceptor interceptor = new SagaAgentInterceptor();
        state.registerCompensation("reserve", s -> s.addFinding("compensated: reserve"));
        state.registerCompensation("charge", s -> s.addFinding("compensated: charge"));
        state.markSagaFailed();
        interceptor.onAfterToolExecution(state, "charge", false); // success=false → unwind
        return "quad-saga unwound; findings=" + state.findings();
    }
}
