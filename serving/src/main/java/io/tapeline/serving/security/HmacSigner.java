package io.tapeline.serving.security;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * HMAC-SHA256 signing and verification.
 *
 * <p>Two details in here are the whole point of writing it rather than
 * inlining {@code Mac.getInstance} at the call site:
 *
 * <ol>
 *   <li>Verification uses {@link MessageDigest#isEqual} rather than
 *       {@code String.equals} or {@code Arrays.equals}. A short-circuiting
 *       comparison leaks, through timing, how many leading bytes of a guess
 *       were correct — which turns forging a signature from infeasible into a
 *       few thousand requests per byte.
 *   <li>The secret is held as bytes and never logged or included in
 *       {@code toString}.
 * </ol>
 */
public final class HmacSigner {

    private static final String ALGORITHM = "HmacSHA256";

    private final byte[] secret;

    public HmacSigner(String secret) {
        if (secret == null || secret.isBlank()) {
            throw new IllegalArgumentException("secret must not be empty");
        }
        this.secret = secret.getBytes(StandardCharsets.UTF_8);
    }

    /** Returns the base64 signature of a canonical request. */
    public String sign(SignedRequest request) {
        return sign(request.canonicalBytes());
    }

    public String sign(byte[] payload) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secret, ALGORITHM));
            return Base64.getEncoder().encodeToString(mac.doFinal(payload));
        } catch (Exception e) {
            throw new IllegalStateException("HMAC computation failed", e);
        }
    }

    /**
     * Constant-time signature check.
     *
     * <p>A malformed base64 candidate returns false rather than throwing:
     * the caller cannot distinguish "wrong signature" from "wrong encoding",
     * which is deliberate. Distinguishable failures are an oracle.
     */
    public boolean verify(SignedRequest request, String candidateBase64) {
        if (candidateBase64 == null || candidateBase64.isBlank()) {
            return false;
        }
        byte[] expected = Base64.getDecoder().decode(sign(request));
        byte[] actual;
        try {
            actual = Base64.getDecoder().decode(candidateBase64);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return MessageDigest.isEqual(expected, actual);
    }

    @Override
    public String toString() {
        // Never let a secret reach a log line through an accidental
        // interpolation of the signer.
        return "HmacSigner{secret=***}";
    }
}
