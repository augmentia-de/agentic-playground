package de.augmentia.agentplatform.orchestrator.web;

public record ApiErrorResponse(ApiError error) {

    public record ApiError(String code, String message, String workflowId) {}
}
