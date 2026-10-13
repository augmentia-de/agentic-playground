package de.augmentia.agentplatform.common.model;

/**
 * Standard agent types used by the reference DAGs / mock registry (SPEC2 §14.1).
 * The worker registers one deterministic mock per type.
 */
public final class AppAgentType {

    public static final String RESEARCHER = "researcher";
    public static final String PLANNER = "planner";
    public static final String CODER = "coder";
    public static final String TESTER = "tester";
    public static final String REVIEWER = "reviewer";
    public static final String FIXER = "fixer";
    public static final String LINTER = "linter";
    public static final String QUALITY = "quality";

    private AppAgentType() {}
}