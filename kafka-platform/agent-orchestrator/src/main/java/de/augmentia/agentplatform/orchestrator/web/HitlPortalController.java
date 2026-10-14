package de.augmentia.agentplatform.orchestrator.web;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.augmentia.agentplatform.common.event.ControlEvents;
import de.augmentia.agentplatform.common.event.EventJson;
import de.augmentia.agentplatform.common.kafka.TopicNames;
import de.augmentia.agentplatform.common.model.HitlAction;
import de.augmentia.agentplatform.orchestrator.domain.HitlTicketRow;
import de.augmentia.agentplatform.orchestrator.kafka.OrchestratorEventPublisher;
import de.augmentia.agentplatform.orchestrator.service.WorkflowQueryService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/hitl")
public class HitlPortalController {

    private static final String PORTAL_HTML = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>HITL Portal - Agent Platform</title>
            <style>
                * { box-sizing: border-box; margin: 0; padding: 0; }
                body {
                    font-family: "MS Sans Serif", "Microsoft Sans Serif", Tahoma, Geneva, sans-serif;
                    background-color: #c0c0c0;
                    color: #000;
                    padding: 16px;
                }
                .win98-titlebar {
                    background: linear-gradient(to right, #000080, #1084d0);
                    color: white;
                    padding: 3px 6px;
                    font-weight: bold;
                    font-size: 12px;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                }
                .win98-window {
                    border: 2px outset #fff;
                    background: #c0c0c0;
                    max-width: 800px;
                    margin: 0 auto;
                }
                .win98-body {
                    padding: 12px;
                }
                h1 {
                    font-size: 14px;
                    margin-bottom: 12px;
                    color: #000080;
                }
                table {
                    width: 100%;
                    border-collapse: collapse;
                    font-size: 11px;
                    background: white;
                    border: 2px inset #fff;
                }
                th {
                    background: #c0c0c0;
                    border: 1px outset #fff;
                    padding: 4px 6px;
                    text-align: left;
                    font-weight: bold;
                    position: sticky;
                    top: 0;
                }
                td {
                    padding: 4px 6px;
                    border: 1px solid transparent;
                }
                tr:hover td {
                    background: #000080;
                    color: white;
                }
                .btn {
                    display: inline-block;
                    padding: 2px 8px;
                    font-size: 11px;
                    font-family: inherit;
                    border: 2px outset #fff;
                    background: #c0c0c0;
                    cursor: pointer;
                    text-decoration: none;
                    color: #000;
                }
                .btn:active {
                    border-style: inset;
                }
                .btn-approve { color: green; }
                .btn-reject { color: red; }
                .btn-retry { color: orange; }
                .btn-abort { color: darkred; }
                .status-open { color: red; font-weight: bold; }
                .status-approved { color: green; }
                .status-skipped { color: gray; }
                .status-aborted { color: darkred; }
                .refresh-info {
                    font-size: 11px;
                    color: #666;
                    margin-bottom: 8px;
                    text-align: right;
                }
                .slab {
                    border: 2px outset #fff;
                    padding: 2px 6px;
                    margin-right: 4px;
                    font-size: 10px;
                    color: #333;
                    background: #c0c0c0;
                }
                .empty-state {
                    text-align: center;
                    padding: 32px;
                    color: #666;
                    font-size: 13px;
                }
            </style>
        </head>
        <body>
            <div class="win98-window">
                <div class="win98-titlebar">
                    <span>Human-in-the-Loop Portal — Agent Platform</span>
                    <div>
                        <span class="slab">_</span>
                        <span class="slab">X</span>
                    </div>
                </div>
                <div class="win98-body">
                    <h1>Open Tickets</h1>
                    <div class="refresh-info">
                        Auto-refreshes every 5 seconds &middot; Last update: <span id="lastUpdate">--</span>
                    </div>
                    <table id="ticketTable">
                        <thead>
                            <tr>
                                <th>Ticket</th>
                                <th>Workflow</th>
                                <th>Step</th>
                                <th>Reason</th>
                                <th>Status</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody id="ticketBody"></tbody>
                    </table>
                    <div id="emptyState" class="empty-state" style="display:none;">
                        No open tickets. Everything is running smoothly!
                    </div>
                </div>
            </div>
            <script>
                const API_BASE = '/api/v1/hitl';
                async function loadTickets() {
                    try {
                        const res = await fetch(API_BASE + '/tickets');
                        const tickets = await res.json();
                        const tbody = document.getElementById('ticketBody');
                        const empty = document.getElementById('emptyState');
                        tbody.innerHTML = '';
                        if (tickets.length === 0) {
                            empty.style.display = 'block';
                        } else {
                            empty.style.display = 'none';
                            tickets.filter(t => t.status === 'OPEN').forEach(ticket => {
                                const tr = document.createElement('tr');
                                tr.innerHTML = \\`
                                    <td><code>\\${ticket.ticketId}</code></td>
                                    <td><code>\\${ticket.workflowExecutionId}</code></td>
                                    <td>\\${ticket.stepId} (iter \\${ticket.iteration})</td>
                                    <td>\\${ticket.reason}</td>
                                    <td class="status-\\${ticket.status.toLowerCase()}">\\${ticket.status}</td>
                                    <td>
                                        <button class="btn btn-approve" onclick="act('approve','\\${ticket.ticketId}','\\${ticket.workflowExecutionId}')">Approve</button>
                                        <button class="btn btn-retry" onclick="act('retry','\\${ticket.ticketId}','\\${ticket.workflowExecutionId}')">Retry</button>
                                        <button class="btn btn-reject" onclick="act('skip','\\${ticket.ticketId}','\\${ticket.workflowExecutionId}')">Skip</button>
                                        <button class="btn btn-abort" onclick="act('abort','\\${ticket.ticketId}','\\${ticket.workflowExecutionId}')">Abort</button>
                                    </td>
                                \\`;
                                tbody.appendChild(tr);
                            });
                        }
                        document.getElementById('lastUpdate').textContent = new Date().toLocaleTimeString();
                    } catch (e) {
                        console.error('Failed to load tickets:', e);
                    }
                }
                async function act(action, ticketId, workflowId) {
                    if (!confirm('Are you sure you want to ' + action + ' this ticket?')) return;
                    const userId = prompt('Your name/id:', window.localStorage.getItem('hitl-user') || '');
                    if (!userId) return;
                    window.localStorage.setItem('hitl-user', userId);
                    try {
                        await fetch(\\`${API_BASE}/tickets/\\${ticketId}/\\${action}\\`, {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({userId: userId})
                        });
                        loadTickets();
                    } catch (e) {
                        alert('Request failed: ' + e.message);
                    }
                }
                loadTickets();
                setInterval(loadTickets, 5000);
            </script>
        </body>
        </html>
        """.trim();

    private final WorkflowQueryService queryService;
    private final OrchestratorEventPublisher publisher;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Value("${app.hitl.slack.signing-secret:}")
    private String slackSigningSecret;

    public HitlPortalController(WorkflowQueryService queryService, OrchestratorEventPublisher publisher) {
        this.queryService = queryService;
        this.publisher = publisher;
    }

    @GetMapping("/portal")
    public ResponseEntity<String> portal() {
        return ResponseEntity.ok()
                .header("Content-Type", "text/html; charset=utf-8")
                .body(PORTAL_HTML);
    }

    /**
     * Slack interactive-callback endpoint. The request is authenticated by verifying Slack's
     * request signature (HMAC over the raw body) — the previous version accepted any payload.
     * The raw body is required for verification, so it is taken as a {@code String} and parsed
     * afterwards.
     */
    @PostMapping("/callback/slack")
    public ResponseEntity<Map<String, String>> slackCallback(
            @RequestBody(required = false) String rawBody,
            @RequestHeader(value = "X-Slack-Signature", required = false) String slackSignature,
            @RequestHeader(value = "X-Slack-Request-Timestamp", required = false) String slackTimestamp) {
        if (rawBody == null || rawBody.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Empty payload"));
        }

        if (!SlackSignatureVerifier.isValid(slackSigningSecret, slackSignature, slackTimestamp,
                rawBody, Instant.now().getEpochSecond())) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(Map.of("error", "Invalid or missing Slack signature"));
        }

        Map<String, Object> payload = parseSlackPayload(rawBody);
        if (payload == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Malformed payload"));
        }

        String value = extractSlackValue(payload);
        if (value == null || value.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Missing 'value' field"));
        }

        Map<String, String> params = parseSlackValue(value);
        String actionStr = params.get("action");
        String ticketIdStr = params.get("ticketId");

        if (actionStr == null || ticketIdStr == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Missing required fields in value"));
        }

        try {
            UUID ticketId = UUID.fromString(ticketIdStr);
            HitlTicketRow ticket = queryService.requireTicket(ticketId);
            HitlAction hitlAction = resolveSlackAction(actionStr);

            ControlEvents.HitlEvent event = new ControlEvents.HitlEvent(
                    ticket.workflowExecutionId().toString(),
                    ticket.id().toString(),
                    ticket.stepId(),
                    ticket.iteration(),
                    hitlAction,
                    "slack:" + extractSlackUserId(payload)
            );
            publisher.publish(TopicNames.HITL, ticket.workflowExecutionId().toString(), EventJson.write(event));
        } catch (Exception ex) {
            return ResponseEntity.badRequest().body(Map.of("error", "Failed to process callback: " + ex.getMessage()));
        }

        Map<String, String> response = new LinkedHashMap<>();
        response.put("response_type", "ephemeral");
        response.put("text", "Action `" + actionStr + "` processed for ticket " + ticketIdStr);
        return ResponseEntity.ok(response);
    }

    private Map<String, Object> parseSlackPayload(String rawBody) {
        try {
            return OBJECT_MAPPER.readValue(rawBody, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private String extractSlackValue(Map<String, Object> payload) {
        if (payload.containsKey("actions")) {
            Object actions = payload.get("actions");
            if (actions instanceof List<?> list && !list.isEmpty()) {
                Map<String, Object> firstAction = (Map<String, Object>) list.get(0);
                Object val = firstAction.get("value");
                return val != null ? val.toString() : null;
            }
        }
        Object value = payload.get("value");
        return value != null ? value.toString() : null;
    }

    @SuppressWarnings("unchecked")
    private String extractSlackUserId(Map<String, Object> payload) {
        Object user = payload.get("user");
        if (user instanceof Map<?,?> m) {
            Object username = m.get("username");
            if (username != null) return username.toString();
        }
        return "unknown";
    }

    private Map<String, String> parseSlackValue(String value) {
        Map<String, String> params = new LinkedHashMap<>();
        if (value == null) return params;
        for (String pair : value.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                params.put(pair.substring(0, eq), pair.substring(eq + 1));
            }
        }
        return params;
    }

    private HitlAction resolveSlackAction(String actionStr) {
        switch (actionStr.toLowerCase(Locale.ROOT)) {
            case "approve", "accept" -> {
                // Use RETRY for approve since it resumes execution
                return HitlAction.RETRY;
            }
            case "reject", "deny" -> {
                // Reject maps to skip (skip the step)
                return HitlAction.SKIP;
            }
            case "retry", "redo" -> {
                return HitlAction.RETRY;
            }
            case "abort", "cancel" -> {
                return HitlAction.ABORT;
            }
            default -> {
                throw new IllegalArgumentException("Unknown Slack action: " + actionStr);
            }
        }
    }
}
