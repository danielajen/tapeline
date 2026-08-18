package io.tapeline.serving.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class SignedRequestTest {

    @Test
    void theCanonicalStringIsNewlineDelimitedInAFixedOrder() {
        SignedRequest r = new SignedRequest("get", "/api/v1/x", 1000L, "n1", "abc");

        assertThat(r.canonicalString()).isEqualTo("GET\n/api/v1/x\n1000\nn1\nabc");
        assertThat(r.method()).as("method is normalized to upper case").isEqualTo("GET");
    }

    /**
     * The reason the canonical string uses a delimiter at all. Without one,
     * two different requests can produce identical signed bytes, and a
     * signature for one becomes a valid signature for the other.
     */
    @Test
    void fieldBoundariesCannotBeShifted() {
        HmacSigner signer = new HmacSigner("secret");

        SignedRequest a = new SignedRequest("GET", "/ab", 1L, "c", "h");
        SignedRequest b = new SignedRequest("GET", "/a", 1L, "bc", "h");

        assertThat(a.canonicalString()).isNotEqualTo(b.canonicalString());
        assertThat(signer.sign(a)).isNotEqualTo(signer.sign(b));
    }

    @Test
    void anEmptyBodyHasAWellDefinedHash() {
        String fromNull = SignedRequest.of("GET", "/x", 1L, "n", null).bodySha256Hex();
        String fromEmpty = SignedRequest.of("GET", "/x", 1L, "n", new byte[0]).bodySha256Hex();

        assertThat(fromNull).isEqualTo(fromEmpty).isEqualTo(SignedRequest.EMPTY_BODY_SHA256);
        // The well-known SHA-256 of the empty string.
        assertThat(fromNull)
                .isEqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    @Test
    void theBodyHashChangesWithTheBody() {
        String one = SignedRequest.of("POST", "/x", 1L, "n", "{\"a\":1}".getBytes(StandardCharsets.UTF_8))
                .bodySha256Hex();
        String two = SignedRequest.of("POST", "/x", 1L, "n", "{\"a\":2}".getBytes(StandardCharsets.UTF_8))
                .bodySha256Hex();

        assertThat(one).isNotEqualTo(two);
    }

    @Test
    void incompleteRequestsAreRejectedAtConstruction() {
        assertThatThrownBy(() -> new SignedRequest("", "/x", 1L, "n", "h"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SignedRequest("GET", "", 1L, "n", "h"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SignedRequest("GET", "/x", 1L, "  ", "h"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SignedRequest("GET", "/x", 1L, "n", null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
