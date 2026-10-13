package de.augmentia.agentflow.examples.support;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/** Steps of the customer-support triage case. */
@ActivityInterface
public interface SupportActivities {

    /** Classifies a ticket into structured {@link TicketAssessment} JSON. */
    @ActivityMethod
    String assessTicket(String message);

    /** Returns the customer's stored history as an injection block. */
    @ActivityMethod
    String recallCustomer(String customerId);

    /** Remembers a support interaction in long-term memory. */
    @ActivityMethod
    String rememberTicket(String customerId, String summary);

    /** Retrieves the most relevant runbooks/tools for the ticket (RAG). */
    @ActivityMethod
    String findPlaybook(String topic);
}
