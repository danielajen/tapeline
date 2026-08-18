package io.tapeline.serving.security;

import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Ties the four checks together in the order that costs least.
 *
 * <p>Ordering matters for more than speed. Signature verification happens
 * <em>before</em> the nonce is consumed, so an unauthenticated caller cannot
 * burn a legitimate client's nonce values. Rate limiting happens last,
 * because charging a token for a request that was never going to be served
 * lets an attacker with no valid key exhaust a real client's quota.
 */
@Component
public class RequestAuthenticator {

    private static final Logger log = LoggerFactory.getLogger(RequestAuthenticator.class);

    /** A single reason enum for logs; clients never see the distinctions. */
    public enum Failure {
        MISSING_CREDENTIALS,
        UNKNOWN_KEY,
        DISABLED_KEY,
        BAD_SIGNATURE,
        STALE_TIMESTAMP,
        REPLAYED_NONCE,
        RATE_LIMITED
    }

    public record Outcome(boolean authenticated, ApiKey key, Failure failure, long retryAfterSeconds) {

        public static Outcome ok(ApiKey key) {
            return new Outcome(true, key, null, 0);
        }

        public static Outcome denied(Failure failure) {
            return new Outcome(false, null, failure, 0);
        }

        public static Outcome throttled(long retryAfterSeconds) {
            return new Outcome(false, null, Failure.RATE_LIMITED, retryAfterSeconds);
        }
    }

    private final ApiKeyRepository keys;
    private final ReplayGuard replayGuard;
    private final TokenBucketRateLimiter rateLimiter;

    public RequestAuthenticator(
            ApiKeyRepository keys, ReplayGuard replayGuard, TokenBucketRateLimiter rateLimiter) {
        this.keys = keys;
        this.replayGuard = replayGuard;
        this.rateLimiter = rateLimiter;
    }

    public Outcome authenticate(Credentials credentials, SignedRequest request, int cost) {
        if (credentials == null || !credentials.isComplete()) {
            return Outcome.denied(Failure.MISSING_CREDENTIALS);
        }

        Optional<ApiKey> maybeKey = keys.findByKeyId(credentials.keyId());
        if (maybeKey.isEmpty()) {
            return Outcome.denied(Failure.UNKNOWN_KEY);
        }
        ApiKey key = maybeKey.get();
        if (!key.isUsable()) {
            return Outcome.denied(Failure.DISABLED_KEY);
        }

        // Signature first: everything after this point has a side effect
        // (consuming a nonce, spending a token) that an unauthenticated
        // caller must not be able to trigger.
        HmacSigner signer = new HmacSigner(key.secret());
        if (!signer.verify(request, credentials.signature())) {
            log.debug("signature mismatch for key {}", key.keyId());
            return Outcome.denied(Failure.BAD_SIGNATURE);
        }

        ReplayGuard.Result replay =
                replayGuard.check(key.keyId(), request.nonce(), request.timestampSeconds());
        switch (replay) {
            case TIMESTAMP_OUT_OF_WINDOW -> {
                return Outcome.denied(Failure.STALE_TIMESTAMP);
            }
            case NONCE_REUSED -> {
                log.warn("nonce replay detected for key {}", key.keyId());
                return Outcome.denied(Failure.REPLAYED_NONCE);
            }
            case OK -> { /* continue */ }
        }

        TokenBucketRateLimiter.Decision decision = rateLimiter.tryConsume(
                key.keyId(), key.rateLimitCapacity(), key.rateLimitRefillPerSecond(), cost);
        if (!decision.allowed()) {
            return Outcome.throttled(decision.retryAfterSeconds());
        }

        return Outcome.ok(key);
    }

    /** The credential triple a client presents. */
    public record Credentials(String keyId, String signature, String nonce, long timestampSeconds) {
        public boolean isComplete() {
            return keyId != null && !keyId.isBlank()
                    && signature != null && !signature.isBlank()
                    && nonce != null && !nonce.isBlank()
                    && timestampSeconds > 0;
        }
    }
}
