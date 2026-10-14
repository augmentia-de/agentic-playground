package de.augmentia.agentplatform.orchestrator.web;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;

/**
 * Verifies Slack request signatures
 * (see <a href="https://api.slack.com/authentication/verifying-requests-from-slack">verifying
 * requests from Slack</a>).
 *
 * <p>The signed base string is {@code v0:<timestamp>:<raw-body>} and the expected header value is
 * {@code v0=<hex hmac-sha256(signingSecret, baseString)>}. A request older than
 * {@link #MAX_TIMESTAMP_SKEW_SECONDS} is rejected to prevent replay.
 *
 * <p>Everything fails closed: a missing secret, signature or timestamp returns {@code false}.
 */
final class SlackSignatureVerifier {

    /** Slack's recommended replay window. */
    static final long MAX_TIMESTAMP_SKEW_SECONDS = 300;

    private SlackSignatureVerifier() {
    }

    static boolean isValid(String signingSecret,
                           String signature,
                           String timestamp,
                           String body,
                           long nowEpochSeconds) {
        if (signingSecret == null || signingSecret.isBlank()
                || signature == null || signature.isBlank()
                || timestamp == null || timestamp.isBlank()
                || body == null) {
            return false;
        }

        long ts;
        try {
            ts = Long.parseLong(timestamp.trim());
        } catch (NumberFormatException e) {
            return false;
        }
        if (Math.abs(nowEpochSeconds - ts) > MAX_TIMESTAMP_SKEW_SECONDS) {
            return false;
        }

        String expected = "v0=" + hmacSha256Hex(signingSecret, "v0:" + timestamp + ":" + body);
        return MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8),
                signature.getBytes(StandardCharsets.UTF_8));
    }

    private static String hmacSha256Hex(String secret, String message) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(message.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException("HmacSHA256 unavailable", e);
        }
    }
}
