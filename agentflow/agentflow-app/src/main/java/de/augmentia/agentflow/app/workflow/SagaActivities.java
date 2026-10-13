package de.augmentia.agentflow.app.workflow;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Forward and compensating steps of a durable saga. Compensations are the reverse of the
 * forward steps; the workflow decides when to run them.
 */
@ActivityInterface
public interface SagaActivities {

    @ActivityMethod
    String reserve(String orderId);

    @ActivityMethod
    String charge(String orderId, double amount);

    @ActivityMethod
    void release(String reservationId);

    @ActivityMethod
    void refund(String orderId);

    /** Demonstrates quad-core's session-level saga (registerCompensation + unwind). */
    @ActivityMethod
    String quadSaga(String input);
}
