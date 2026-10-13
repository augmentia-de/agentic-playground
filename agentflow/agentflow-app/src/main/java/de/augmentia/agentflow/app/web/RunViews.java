package de.augmentia.agentflow.app.web;

import io.temporal.api.enums.v1.WorkflowExecutionStatus;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowExecutionDescription;
import io.temporal.client.WorkflowExecutionMetadata;
import io.temporal.client.WorkflowStub;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** Shared Temporal visibility views used by the REST controller and the SSE streams. */
@Component
public class RunViews {

    private final WorkflowClient workflowClient;

    public RunViews(WorkflowClient workflowClient) {
        this.workflowClient = workflowClient;
    }

    public List<Map<String, Object>> list(String query, int limit) {
        int max = Math.max(1, Math.min(limit, 500));
        try (Stream<WorkflowExecutionMetadata> stream = workflowClient.listExecutions(query)) {
            return stream.limit(max).map(this::toMap).toList();
        } catch (RuntimeException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Temporal visibility unavailable: " + e.getMessage(), e);
        }
    }

    public Map<String, Object> detail(String workflowId) {
        WorkflowStub stub = workflowClient.newUntypedWorkflowStub(workflowId);
        WorkflowExecutionDescription description;
        try {
            description = stub.describe();
        } catch (RuntimeException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown run: " + workflowId, e);
        }
        Map<String, Object> map = new LinkedHashMap<>(toMap(description));
        map.put("result", resultOf(stub, description));
        return map;
    }

    public void terminate(String workflowId, String reason) {
        WorkflowExecutionStatus status;
        try {
            status = workflowClient.newUntypedWorkflowStub(workflowId).describe().getStatus();
        } catch (RuntimeException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown run: " + workflowId, e);
        }
        if (status != WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Run " + workflowId + " is not active (" + shortStatus(status) + ") — nothing to terminate.");
        }
        try {
            workflowClient.newUntypedWorkflowStub(workflowId).terminate(reason);
        } catch (RuntimeException e) {
            // The run may have closed between the status check and the terminate call.
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Run " + workflowId + " could not be terminated: " + e.getMessage(), e);
        }
    }

    /** Returns the (last {@code max}) history events of a run as compact log entries. */
    public List<Map<String, Object>> events(String workflowId, int max) {
        int cap = Math.max(1, Math.min(max, 2000));
        try (Stream<HistoryEvent> stream = workflowClient.streamHistory(workflowId)) {
            List<HistoryEvent> all = stream.toList();
            int from = Math.max(0, all.size() - cap);
            List<Map<String, Object>> out = new ArrayList<>();
            for (int i = from; i < all.size(); i++) {
                out.add(eventView(all.get(i)));
            }
            return out;
        } catch (RuntimeException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No history for run: " + workflowId, e);
        }
    }

    private Map<String, Object> eventView(HistoryEvent event) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("eventId", event.getEventId());
        map.put("type", event.getEventType().name().replace("EVENT_TYPE_", ""));
        map.put("time", event.getEventTime() == null ? null
                : Instant.ofEpochSecond(event.getEventTime().getSeconds(), event.getEventTime().getNanos()).toString());
        map.put("detail", detail(event));
        return map;
    }

    private String detail(HistoryEvent event) {
        if (event.hasWorkflowExecutionStartedEventAttributes()) {
            return event.getWorkflowExecutionStartedEventAttributes().getWorkflowType().getName();
        }
        if (event.hasActivityTaskScheduledEventAttributes()) {
            return event.getActivityTaskScheduledEventAttributes().getActivityType().getName();
        }
        if (event.hasActivityTaskCompletedEventAttributes()) {
            return "completed";
        }
        if (event.hasActivityTaskFailedEventAttributes()) {
            return "failed";
        }
        return "";
    }

    public boolean isRunning(String workflowId) {
        try {
            return workflowClient.newUntypedWorkflowStub(workflowId).describe().getStatus()
                    == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private String resultOf(WorkflowStub stub, WorkflowExecutionDescription description) {
        if (description.getStatus() == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING) {
            return null;
        }
        try {
            return stub.getResult(String.class);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private Map<String, Object> toMap(WorkflowExecutionMetadata meta) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("workflowId", meta.getExecution().getWorkflowId());
        map.put("runId", meta.getExecution().getRunId());
        map.put("workflowType", meta.getWorkflowType());
        map.put("taskQueue", meta.getTaskQueue());
        map.put("status", shortStatus(meta.getStatus()));
        map.put("startTime", toIso(meta.getStartTime()));
        map.put("closeTime", toIso(meta.getCloseTime()));
        map.put("historyLength", meta.getHistoryLength());
        return map;
    }

    static String shortStatus(WorkflowExecutionStatus status) {
        return status == null ? "UNKNOWN" : status.name().replace("WORKFLOW_EXECUTION_STATUS_", "");
    }

    private static String toIso(Instant instant) {
        return instant == null ? null : instant.toString();
    }
}
