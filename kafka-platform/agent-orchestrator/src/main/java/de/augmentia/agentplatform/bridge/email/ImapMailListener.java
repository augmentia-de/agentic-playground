package de.augmentia.agentplatform.bridge.email;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.Store;
import java.util.Properties;
import java.util.UUID;

/**
 * Polls an IMAP mailbox for inbound emails, deduplicates by message-ID header,
 * and hands each new email to {@link EmailInputTransformer}, which starts the
 * referenced workflow through the canonical orchestrator start path.
 */
@Component
@ConditionalOnProperty(name = "app.email.imap.enabled", havingValue = "true")
public class ImapMailListener {

    private static final Logger log = LoggerFactory.getLogger(ImapMailListener.class);

    private static final String INTAKE_LOOKUP_PREFIX = "email:intake:";
    private static final long DEDUP_TTL_SECONDS = 604800L; // 7 days

    @Value("${app.email.imap.host:localhost}")
    private String imapHost;

    @Value("${app.email.imap.port:993}")
    private int imapPort;

    @Value("${app.email.imap.user:}")
    private String imapUser;

    @Value("${app.email.imap.password:}")
    private String imapPassword;

    @Value("${app.email.imap.folder:INBOX}")
    private String imapFolder;

    @Value("${app.email.imap.dedup-window-seconds:3600}")
    private long dedupWindowSeconds;

    @Value("${app.email.imap.poll-interval-ms:5000}")
    private long pollIntervalMs;

    @Value("${app.email.imap.ssl-enabled:true}")
    private boolean sslEnabled;

    private final EmailInputTransformer emailTransformer;
    private final StringRedisTemplate redis;

    public ImapMailListener(EmailInputTransformer emailTransformer, StringRedisTemplate redis) {
        this.emailTransformer = emailTransformer;
        this.redis = redis;
    }

    @Scheduled(fixedDelayString = "${app.email.imap.poll-interval-ms:5000}")
    public void poll() {
        if (!canConnect()) {
            return;
        }

        Store store = null;
        Folder folder = null;
        try {
            Session session = buildSession();
            store = session.getStore("imap");
            store.connect(imapHost, imapPort, imapUser, imapPassword);

            folder = store.getFolder(imapFolder);
            folder.open(Folder.READ_ONLY);

            Message[] messages = folder.getMessages();
            for (Message msg : messages) {
                processMessage(msg);
            }
        } catch (MessagingException e) {
            log.warn("IMAP poll failed: {} — will retry at next interval", e.getMessage());
        } finally {
            tryClose(folder);
            tryCloseStore(store);
        }
    }

    private boolean canConnect() {
        return imapUser != null && !imapUser.isBlank() && imapPassword != null && !imapPassword.isBlank();
    }

    private Session buildSession() {
        Properties props = new Properties();
        props.put("mail.imap.host", imapHost);
        props.put("mail.imap.port", String.valueOf(imapPort));
        props.put("mail.store.protocol", "imap");
        if (sslEnabled) {
            props.put("mail.imap.ssl.enable", "true");
            props.put("mail.imap.ssl.trust", imapHost);
        }
        return Session.getInstance(props);
    }

    private void processMessage(Message message) {
        try {
            String messageId = getMessageId(message);
            if (messageId == null || isAlreadyProcessed(messageId)) {
                return;
            }

            markProcessed(messageId);

            if (message.getReceivedDate() == null) {
                log.debug("Skipping message without received date: {}", messageId);
                return;
            }

            String from = extractAddress(message.getFrom());
            String subject = message.getSubject() != null ? message.getSubject() : "";
            String body = safeExtractBody(message);

            EmailInputTransformer.WebhookPayload payload =
                    new EmailInputTransformer.WebhookPayload(imapUser, from, subject, body, null);
            emailTransformer.receiveEmail(payload);
            log.info("Processed email: from={}, subject={}", from, subject);
        } catch (Exception e) {
            log.error("Failed to process message: {}", e.getMessage());
        }
    }

    private String getMessageId(Message message) {
        try {
            String[] ids = message.getHeader("Message-ID");
            return ids != null && ids.length > 0 ? ids[0] : null;
        } catch (MessagingException e) {
            return null;
        }
    }

    private boolean isAlreadyProcessed(String messageId) {
        String key = INTAKE_LOOKUP_PREFIX + messageId;
        Boolean exists = redis.hasKey(key);
        return Boolean.TRUE.equals(exists);
    }

    private void markProcessed(String messageId) {
        String key = INTAKE_LOOKUP_PREFIX + messageId;
        redis.opsForValue().set(key, UUID.randomUUID().toString(),
                java.time.Duration.ofSeconds(dedupWindowSeconds));
    }

    private String extractAddress(jakarta.mail.Address[] addresses) {
        if (addresses == null || addresses.length == 0) return "unknown";
        if (addresses[0] instanceof jakarta.mail.internet.InternetAddress ia) {
            return ia.getAddress() != null ? ia.getAddress() : ia.toString();
        }
        return addresses[0].toString();
    }

    private String safeExtractBody(Message message) {
        try {
            if (message.isMimeType("text/plain")) {
                return String.valueOf(message.getContent());
            }
            if (message.isMimeType("text/html")) {
                return stripHtml(String.valueOf(message.getContent()));
            }
            Object content = message.getContent();
            if (content instanceof jakarta.mail.Multipart mp) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < mp.getCount(); i++) {
                    jakarta.mail.BodyPart part = mp.getBodyPart(i);
                    if (part.isMimeType("text/plain")) {
                        if (sb.length() > 0) sb.append('\n');
                        sb.append(part.getContent());
                    } else if (part.isMimeType("text/html")) {
                        if (sb.length() > 0) sb.append('\n');
                        sb.append(stripHtml(part.getContent().toString()));
                    }
                }
                return sb.length() > 0 ? sb.toString() : "(multipart with no text part)";
            }
            return "(non-text message type: " + message.getContentType() + ")";
        } catch (Exception e) {
            log.warn("Failed to extract email body: {}", e.getMessage());
            return "(failed to extract body: " + e.getClass().getSimpleName() + ")";
        }
    }

    private String stripHtml(String html) {
        if (html == null) return "";
        return html.replaceAll("<[^>]*>", "").replaceAll("\\s+", " ").trim();
    }

    private void tryClose(Folder folder) {
        if (folder != null && folder.isOpen()) {
            try {
                folder.close(false);
            } catch (MessagingException e) {
                log.debug("Error closing folder: {}", e.getMessage());
            }
        }
    }

    private void tryCloseStore(Store store) {
        if (store != null && store.isConnected()) {
            try {
                store.close();
            } catch (MessagingException e) {
                log.debug("Error closing store: {}", e.getMessage());
            }
        }
    }
}
