package io.tapeline.serving.security;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Replay protection: a timestamp window plus a single-use nonce.
 *
 * <p>Neither half works alone, which is the part worth being explicit about.
 *
 * <ul>
 *   <li><b>The timestamp window alone</b> lets an attacker replay a captured
 *       request freely for as long as the window is open.
 *   <li><b>The nonce alone</b> would require remembering every nonce ever
 *       seen, forever — unbounded storage, and the reason naive nonce schemes
 *       get quietly capped and then silently stop protecting anything.
 * </ul>
 *
 * <p>Together they bound each other: the window limits how long a nonce must
 * be remembered, so the nonce set stays small and can expire on its own. The
 * Redis key TTL is therefore set from the window, not chosen independently —
 * a TTL shorter than the window would reopen the replay hole.
 */
@Component
public class ReplayGuard {

    /** How far a client's clock may be from the server's. */
    public static final Duration DEFAULT_SKEW = Duration.ofSeconds(30);

    private static final String KEY_PREFIX = "tapeline:nonce:";

    private final StringRedisTemplate redis;
    private final Clock clock;
    private final Duration skew;

    /**
     * The constructor Spring uses.
     *
     * <p>Annotated explicitly because this class has two. Without the
     * annotation Spring cannot choose between them, falls back to looking for
     * a no-arg constructor, finds none, and the context fails to start with
     * {@code NoSuchMethodException: ReplayGuard.<init>()} — an error that
     * names the symptom and not the cause.
     *
     * <p>The unit tests never caught this because they construct the class
     * directly. It only appeared the first time the application was actually
     * started.
     */
    @Autowired
    public ReplayGuard(StringRedisTemplate redis) {
        this(redis, Clock.systemUTC(), DEFAULT_SKEW);
    }

    public ReplayGuard(StringRedisTemplate redis, Clock clock, Duration skew) {
        this.redis = redis;
        this.clock = clock;
        this.skew = skew;
    }

    /** The outcome of a replay check, kept distinct so the caller can log the
     * reason without returning it to the client. */
    public enum Result {
        OK,
        /** The request is older than the window, or dated in the future. */
        TIMESTAMP_OUT_OF_WINDOW,
        /** This (key, nonce) pair has already been used. */
        NONCE_REUSED
    }

    /**
     * Checks and consumes a nonce.
     *
     * <p>Consuming is atomic: {@code SET key NX} either claims the nonce or
     * reports that someone already has. A read-then-write would let two
     * concurrent copies of a replayed request both pass, which is precisely
     * the case an attacker would engineer.
     *
     * <p>The nonce is namespaced by API key, so one tenant cannot deny
     * another service by burning nonce values.
     */
    public Result check(String apiKeyId, String nonce, long timestampSeconds) {
        long now = clock.instant().getEpochSecond();
        long delta = Math.abs(now - timestampSeconds);
        if (delta > skew.toSeconds()) {
            return Result.TIMESTAMP_OUT_OF_WINDOW;
        }

        String key = KEY_PREFIX + apiKeyId + ":" + nonce;

        // TTL is twice the window: a request timestamped at the far edge of
        // the allowed skew must stay un-replayable for the remainder of the
        // window on both sides.
        Boolean claimed = redis.opsForValue()
                .setIfAbsent(key, "1", skew.toSeconds() * 2, TimeUnit.SECONDS);

        return Boolean.TRUE.equals(claimed) ? Result.OK : Result.NONCE_REUSED;
    }

    public Duration skew() {
        return skew;
    }
}
