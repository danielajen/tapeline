package io.tapeline.serving.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class HmacSignerTest {

    private static final String SECRET = "s3cr3t-key-material-not-a-real-one";

    private SignedRequest request() {
        return SignedRequest.of(
                "GET", "/api/v1/quotes/BTC-USD", 1_755_400_000L, "nonce-1", null);
    }

    @Test
    void signingIsDeterministic() {
        HmacSigner signer = new HmacSigner(SECRET);
        assertThat(signer.sign(request())).isEqualTo(signer.sign(request()));
    }

    @Test
    void differentSecretsProduceDifferentSignatures() {
        assertThat(new HmacSigner(SECRET).sign(request()))
                .isNotEqualTo(new HmacSigner(SECRET + "x").sign(request()));
    }

    @Test
    void aValidSignatureVerifies() {
        HmacSigner signer = new HmacSigner(SECRET);
        assertThat(signer.verify(request(), signer.sign(request()))).isTrue();
    }

    @Test
    void aTamperedSignatureFails() {
        HmacSigner signer = new HmacSigner(SECRET);
        String valid = signer.sign(request());
        String tampered = (valid.charAt(0) == 'A' ? "B" : "A") + valid.substring(1);

        assertThat(signer.verify(request(), tampered)).isFalse();
    }

    @Test
    void aSignatureIsBoundToEveryFieldOfTheRequest() {
        HmacSigner signer = new HmacSigner(SECRET);
        String signature = signer.sign(request());

        // Each of these is a distinct attack: replaying a GET signature on a
        // DELETE, on another symbol, at a later time, or with a fresh nonce.
        assertThat(signer.verify(
                        SignedRequest.of("DELETE", "/api/v1/quotes/BTC-USD", 1_755_400_000L, "nonce-1", null),
                        signature))
                .as("method must be bound")
                .isFalse();

        assertThat(signer.verify(
                        SignedRequest.of("GET", "/api/v1/quotes/ETH-USD", 1_755_400_000L, "nonce-1", null),
                        signature))
                .as("path must be bound")
                .isFalse();

        assertThat(signer.verify(
                        SignedRequest.of("GET", "/api/v1/quotes/BTC-USD", 1_755_400_001L, "nonce-1", null),
                        signature))
                .as("timestamp must be bound")
                .isFalse();

        assertThat(signer.verify(
                        SignedRequest.of("GET", "/api/v1/quotes/BTC-USD", 1_755_400_000L, "nonce-2", null),
                        signature))
                .as("nonce must be bound")
                .isFalse();

        assertThat(signer.verify(
                        SignedRequest.of(
                                "GET", "/api/v1/quotes/BTC-USD", 1_755_400_000L, "nonce-1",
                                "{\"evil\":true}".getBytes(StandardCharsets.UTF_8)),
                        signature))
                .as("body must be bound")
                .isFalse();
    }

    @Test
    void malformedBase64IsRejectedRatherThanThrowing() {
        HmacSigner signer = new HmacSigner(SECRET);
        // A caller must not be able to distinguish a bad encoding from a bad
        // signature — and must not be able to cause a 500 either.
        assertThat(signer.verify(request(), "!!!not base64!!!")).isFalse();
        assertThat(signer.verify(request(), "")).isFalse();
        assertThat(signer.verify(request(), null)).isFalse();
    }

    @Test
    void aTruncatedSignatureDoesNotVerify() {
        HmacSigner signer = new HmacSigner(SECRET);
        String valid = signer.sign(request());
        // Base64 of a shorter byte array is still valid base64, so this must
        // be caught by the length-sensitive comparison, not by decoding.
        assertThat(signer.verify(request(), valid.substring(0, 8))).isFalse();
    }

    @Test
    void anEmptySecretIsRejected() {
        assertThatThrownBy(() -> new HmacSigner(""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HmacSigner(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theSecretNeverAppearsInToString() {
        assertThat(new HmacSigner(SECRET).toString()).doesNotContain(SECRET);
    }
}
