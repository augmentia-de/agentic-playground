package de.augmentia.agentplatform.orchestrator.web;

import de.augmentia.agentplatform.orchestrator.service.DuplicateWorkflowDefinitionException;
import de.augmentia.agentplatform.orchestrator.service.WorkflowDefinitionNotFoundException;
import de.augmentia.agentplatform.orchestrator.service.WorkflowNotFoundException;
import de.augmentia.agentplatform.orchestrator.service.WorkflowValidationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(WorkflowNotFoundException.class)
    public ResponseEntity<ApiErrorResponse> notFound(WorkflowNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ApiErrorResponse(new ApiErrorResponse.ApiError("WORKFLOW_NOT_FOUND", ex.getMessage(), ex.workflowId())));
    }

    @ExceptionHandler(WorkflowDefinitionNotFoundException.class)
    public ResponseEntity<ApiErrorResponse> definitionNotFound(WorkflowDefinitionNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ApiErrorResponse(new ApiErrorResponse.ApiError("WORKFLOW_DEFINITION_NOT_FOUND", ex.getMessage(), null)));
    }

    @ExceptionHandler(DuplicateWorkflowDefinitionException.class)
    public ResponseEntity<ApiErrorResponse> duplicateDefinition(DuplicateWorkflowDefinitionException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ApiErrorResponse(new ApiErrorResponse.ApiError("WORKFLOW_DEFINITION_EXISTS", ex.getMessage(), null)));
    }

    @ExceptionHandler(WorkflowValidationException.class)
    public ResponseEntity<ApiErrorResponse> invalidDefinition(WorkflowValidationException ex) {
        return ResponseEntity.badRequest()
                .body(new ApiErrorResponse(new ApiErrorResponse.ApiError("WORKFLOW_DEFINITION_INVALID",
                        String.join("; ", ex.violations()), null)));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiErrorResponse> unreadable(HttpMessageNotReadableException ex) {
        return ResponseEntity.badRequest()
                .body(new ApiErrorResponse(new ApiErrorResponse.ApiError("BAD_REQUEST", ex.getMessage(), null)));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiErrorResponse> badRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest()
                .body(new ApiErrorResponse(new ApiErrorResponse.ApiError("BAD_REQUEST", ex.getMessage(), null)));
    }

    /**
     * A non-existent endpoint is an error of the caller, not an outage.
     *
     * <p>Without this handler {@code NoResourceFoundException} lands in
     * {@link #internal(Exception)} and comes back as HTTP 500 with the message
     * {@code "No static resource ... for request '/api/v1/..."}. That is wrong in two
     * places: the caller holds the platform to be broken where it merely does not know an
     * address — and the response names the internal path, which does not belong outside
     * for troubleshooting.
     */
    @ExceptionHandler(org.springframework.web.servlet.resource.NoResourceFoundException.class)
    public ResponseEntity<ApiErrorResponse> notFoundResource(
            org.springframework.web.servlet.resource.NoResourceFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ApiErrorResponse(new ApiErrorResponse.ApiError("NOT_FOUND",
                        "Unbekannter Endpunkt", null)));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiErrorResponse> internal(Exception ex) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ApiErrorResponse(new ApiErrorResponse.ApiError("INTERNAL_ERROR", ex.getMessage(), null)));
    }
}
