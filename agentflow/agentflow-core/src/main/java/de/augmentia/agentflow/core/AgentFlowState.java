package de.augmentia.agentflow.core;

import org.bsc.langgraph4j.state.AgentState;

import java.util.Map;
import java.util.Optional;

/**
 * Shared graph state for an AgentFlow run.
 *
 * <p>Overwrite semantics (no custom reducers): each node returns the keys it changes.
 * Kept intentionally small so the state can be checkpointed cheaply.</p>
 */
public class AgentFlowState extends AgentState implements java.io.Serializable {

    public static final String INPUT = "input";
    public static final String PLAN = "plan";
    public static final String RESULT = "result";
    public static final String REVIEW = "review";
    public static final String ATTEMPTS = "attempts";
    public static final String TRACE = "trace";
    /** Human decision injected on resume (HITL). */
    public static final String APPROVAL = "approval";

    public AgentFlowState(Map<String, Object> initData) {
        super(initData);
    }

    public String string(String key) {
        return value(key).map(Object::toString).orElse("");
    }

    public String input() {
        return string(INPUT);
    }

    public String plan() {
        return string(PLAN);
    }

    public String result() {
        return string(RESULT);
    }

    public String review() {
        return string(REVIEW);
    }

    public int attempts() {
        Optional<Object> v = value(ATTEMPTS);
        return v.map(o -> (o instanceof Number n) ? n.intValue() : Integer.parseInt(o.toString())).orElse(0);
    }

    public String trace() {
        return string(TRACE);
    }

    /** Human approval decision, if present in the state. */
    public boolean approval() {
        return value(APPROVAL).map(o -> (o instanceof Boolean b) ? b : Boolean.parseBoolean(o.toString())).orElse(false);
    }
}
