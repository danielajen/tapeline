package io.tapeline.serving.security;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * The canonical string that a client signs and the server re-derives.
 *
 * <p>Getting the canonical form right is the entire security property. The
 * rules below are not arbitrary:
 *
 * <ul>
 *   <li><b>The method and path are included</b>, so a signature captured from
 *       a GET cannot be replayed against a DELETE on the same path.
 *   <li><b>The body hash is included, not the body</b>, so signing does not
 *       require buffering an arbitrarily large payload twice, and so the
 *       canonical string stays a fixed size.
 *   <li><b>The timestamp and nonce are included</b>, which is what makes the
 *       replay defences meaningful — without them in the signed material, an
 *       attacker could simply supply a fresh timestamp with a stolen
 *       signature.
 *   <li><b>Fields are joined with a newline</b> rather than concatenated. A
 *       delimiter-free join is vulnerable to a boundary-shifting attack:
 *       ("/ab", "c") and ("/a", "bc") would produce the same signed bytes.
 * </ul>
 *
 * <p>This mirrors how Coinbase's own API authentication works, which is the
 * point — it is the scheme the domain actually uses.
 */
public record SignedRequest(
        String method,
        String path,
        long timestampSeconds,
        String nonce,
        String bodySha256Hex) {

    private static final String FIELD_SEPARATOR = "\n";

    public SignedRequest {
        if (method == null || method.isBlank()) {
            throw new IllegalArgumentException("method is required");
        }
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        if (nonce == null || nonce.isBlank()) {
            throw new IllegalArgumentException("nonce is required");
        }
        if (bodySha256Hex == null) {
            throw new IllegalArgumentException("body hash is required (use the empty-body hash)");
        }
        method = method.toUpperCase();
    }

    /** Builds a signed request from raw body bytes. */
    public static SignedRequest of(
            String method, String path, long timestampSeconds, String nonce, byte[] body) {
        return new SignedRequest(method, path, timestampSeconds, nonce, sha256Hex(body));
    }

    /** The bytes actually passed to HMAC. */
    public String canonicalString() {
        return String.join(
                FIELD_SEPARATOR,
                method,
                path,
                Long.toString(timestampSeconds),
                nonce,
                bodySha256Hex);
    }

    public byte[] canonicalBytes() {
        return canonicalString().getBytes(StandardCharsets.UTF_8);
    }

    /** SHA-256 of a body, hex encoded. A null or empty body hashes the empty
     * string, so GET requests have a well-defined value rather than a special
     * case that clients get wrong. */
    public static String sha256Hex(byte[] body) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(body == null ? new byte[0] : body));
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is required of every JVM; this cannot happen.
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    /** The hash of an empty body, precomputed for convenience. */
    public static final String EMPTY_BODY_SHA256 = sha256Hex(new byte[0]);
}
