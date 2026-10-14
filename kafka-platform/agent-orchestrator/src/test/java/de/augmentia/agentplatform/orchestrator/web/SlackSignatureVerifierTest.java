package de.augmentia.agentplatform.orchestrator.web;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WP02 / B14 — Slack request signature verification.
 */
class SlackSignatureVerifierTest {

    private static final String SECRET = "8f742231b10e8888abcd99yyyzzz85a5";
    private static final String BODY = "{\"actions\":[{\"value\":\"action=approve\"}]}";
    private static final long NOW = 1_700_000_000L;

    private static String signature(String secret, String timestamp, String body) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            String base = "v0:" + timestamp + ":" + body;
            return "v0=" + HexFormat.of().formatHex(mac.doFinal(base.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    @DisplayName("A correctly signed request is accepted")
    void validSignatureAccepted() {
        String ts = String.valueOf(NOW);
        String sig = signature(SECRET, ts, BODY);

        assertThat(SlackSignatureVerifier.isValid(SECRET, sig, ts, BODY, NOW)).isTrue();
    }

    @Test
    @DisplayName("A wrong signature is rejected")
    void wrongSignatureRejected() {
        String ts = String.valueOf(NOW);

        assertThat(SlackSignatureVerifier.isValid(SECRET, "v0=deadbeef", ts, BODY, NOW)).isFalse();
    }

    @Test
    @DisplayName("A tampered body invalidates the signature")
    void tamperedBodyRejected() {
        String ts = String.valueOf(NOW);
        String sig = signature(SECRET, ts, BODY);

        assertThat(SlackSignatureVerifier.isValid(SECRET, sig, ts, BODY + " ", NOW)).isFalse();
    }

    @Test
    @DisplayName("A stale timestamp (replay) is rejected")
    void staleTimestampRejected() {
        String ts = String.valueOf(NOW - 10_000);
        String sig = signature(SECRET, ts, BODY);

        assertThat(SlackSignatureVerifier.isValid(SECRET, sig, ts, BODY, NOW)).isFalse();
    }

    @Test
    @DisplayName("Missing secret, signature or timestamp fails closed")
    void missingPartsFailClosed() {
        String ts = String.valueOf(NOW);
        String sig = signature(SECRET, ts, BODY);

        assertThat(SlackSignatureVerifier.isValid(null, sig, ts, BODY, NOW)).isFalse();
        assertThat(SlackSignatureVerifier.isValid("", sig, ts, BODY, NOW)).isFalse();
        assertThat(SlackSignatureVerifier.isValid(SECRET, null, ts, BODY, NOW)).isFalse();
        assertThat(SlackSignatureVerifier.isValid(SECRET, sig, null, BODY, NOW)).isFalse();
        assertThat(SlackSignatureVerifier.isValid(SECRET, sig, "not-a-number", BODY, NOW)).isFalse();
    }
}
