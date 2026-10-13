package de.augmentia.agentflow.app.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Server-Sent Events for live run updates. Emits the run list (or a single run) as
 * {@code text/event-stream}; clients reconnect automatically. SSE is a best-effort push — the UI
 * still has a polling fallback.
 */
@RestController
public class RunStreamController {

    private final RunViews runs;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "run-stream");
        t.setDaemon(true);
        return t;
    });

    public RunStreamController(RunViews runs) {
        this.runs = runs;
    }

    /** Streams the run list (Temporal visibility) every {@code intervalMs}. */
    @GetMapping(value = "/agentflow/runs/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamList(@RequestParam(name = "query", defaultValue = "") String query,
                                 @RequestParam(name = "limit", defaultValue = "200") int limit,
                                 @RequestParam(name = "intervalMs", defaultValue = "2000") long intervalMs) {
        SseEmitter emitter = new SseEmitter(0L);
        AtomicReference<ScheduledFuture<?>> task = new AtomicReference<>();
        task.set(scheduler.scheduleAtFixedRate(() -> {
            try {
                List<Map<String, Object>> list = runs.list(query, limit);
                emitter.send(SseEmitter.event().name("runs").data(mapper.writeValueAsString(list)));
            } catch (Exception e) {
                emitter.completeWithError(e);
            }
        }, 0, Math.max(500, intervalMs), TimeUnit.MILLISECONDS));
        onClose(emitter, task);
        return emitter;
    }

    /** Streams a single run's status/result and its new history events until it is no longer running. */
    @GetMapping(value = "/agentflow/runs/{workflowId}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamRun(@PathVariable("workflowId") String workflowId,
                                @RequestParam(name = "intervalMs", defaultValue = "1500") long intervalMs) {
        SseEmitter emitter = new SseEmitter(0L);
        AtomicReference<ScheduledFuture<?>> task = new AtomicReference<>();
        AtomicLong lastEventId = new AtomicLong(0);
        task.set(scheduler.scheduleAtFixedRate(() -> {
            try {
                Map<String, Object> detail = runs.detail(workflowId);
                emitter.send(SseEmitter.event().name("run").data(mapper.writeValueAsString(detail)));

                List<Map<String, Object>> events = runs.events(workflowId, 1000);
                List<Map<String, Object>> fresh = events.stream()
                        .filter(e -> ((Number) e.get("eventId")).longValue() > lastEventId.get())
                        .toList();
                if (!fresh.isEmpty()) {
                    long maxId = fresh.stream()
                            .mapToLong(e -> ((Number) e.get("eventId")).longValue())
                            .max().orElse(lastEventId.get());
                    lastEventId.set(maxId);
                    emitter.send(SseEmitter.event().name("events").data(mapper.writeValueAsString(fresh)));
                }

                if (!"RUNNING".equals(detail.get("status"))) {
                    emitter.complete();
                }
            } catch (IOException | RuntimeException e) {
                emitter.completeWithError(e);
            }
        }, 0, Math.max(500, intervalMs), TimeUnit.MILLISECONDS));
        onClose(emitter, task);
        return emitter;
    }

    private void onClose(SseEmitter emitter, AtomicReference<ScheduledFuture<?>> task) {
        Runnable cancel = () -> {
            ScheduledFuture<?> f = task.get();
            if (f != null) {
                f.cancel(false);
            }
        };
        emitter.onCompletion(cancel);
        emitter.onTimeout(cancel);
        emitter.onError(e -> cancel.run());
    }
}
