package io.tapeline.serving.security;

import java.util.List;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * A distributed token bucket, evaluated inside Redis.
 *
 * <p><b>Why Lua and not three Redis calls.</b> The obvious implementation —
 * read the bucket, compute the refill, write it back — is a read-modify-write
 * across a network. With more than one serving replica, two concurrent
 * requests both read the same token count and both decrement it, so the
 * effective limit becomes {@code limit * replicas} under exactly the load
 * that makes the limit matter. Running the whole computation as a script
 * makes it atomic on the single-threaded Redis event loop, so the limit holds
 * regardless of replica count.
 *
 * <p><b>Why a token bucket and not a fixed window.</b> A fixed window admits
 * a double-rate burst across its boundary: a client can spend its full quota
 * in the last instant of one window and again in the first instant of the
 * next. A token bucket refills continuously, so the burst is bounded by the
 * bucket capacity and nothing else.
 */
@Component
public class TokenBucketRateLimiter {

    private static final String KEY_PREFIX = "tapeline:ratelimit:";

    /**
     * Refill, then attempt to spend, then persist — atomically.
     *
     * <p>The TTL is refreshed on every call and set from the time it would
     * take to refill the bucket from empty. An idle client's key therefore
     * expires on its own; without that, every API key ever issued would leave
     * a permanent key in Redis.
     */
    private static final String SCRIPT =
            """
            local key = KEYS[1]
            local capacity = tonumber(ARGV[1])
            local refill_per_sec = tonumber(ARGV[2])
            local now_ms = tonumber(ARGV[3])
            local cost = tonumber(ARGV[4])

            local bucket = redis.call('HMGET', key, 'tokens', 'updated_ms')
            local tokens = tonumber(bucket[1])
            local updated_ms = tonumber(bucket[2])

            if tokens == nil then
              tokens = capacity
              updated_ms = now_ms
            end

            -- Refill for elapsed time, clamped at capacity. A clock that goes
            -- backwards must not mint tokens, hence the max with 0.
            local elapsed = math.max(0, now_ms - updated_ms) / 1000.0
            tokens = math.min(capacity, tokens + elapsed * refill_per_sec)

            local allowed = 0
            if tokens >= cost then
              tokens = tokens - cost
              allowed = 1
            end

            redis.call('HSET', key, 'tokens', tokens, 'updated_ms', now_ms)
            local ttl = math.ceil(capacity / refill_per_sec) + 1
            redis.call('EXPIRE', key, ttl)

            -- Seconds until one more token is available, for Retry-After.
            local retry_after = 0
            if allowed == 0 then
              retry_after = math.ceil((cost - tokens) / refill_per_sec)
            end

            return { allowed, math.floor(tokens), retry_after }
            """;

    private final StringRedisTemplate redis;
    private final RedisScript<List> script;

    public TokenBucketRateLimiter(StringRedisTemplate redis) {
        this.redis = redis;
        this.script = new DefaultRedisScript<>(SCRIPT, List.class);
    }

    /** The verdict, including what a client needs to back off correctly. */
    public record Decision(boolean allowed, long remainingTokens, long retryAfterSeconds) {}

    public Decision tryConsume(String apiKeyId, int capacity, double refillPerSecond) {
        return tryConsume(apiKeyId, capacity, refillPerSecond, 1);
    }

    /**
     * @param cost tokens this request consumes. A streaming subscription is
     *     charged more than a single quote read, because it costs more to
     *     serve for as long as it lives.
     */
    @SuppressWarnings("unchecked")
    public Decision tryConsume(String apiKeyId, int capacity, double refillPerSecond, int cost) {
        if (capacity <= 0 || refillPerSecond <= 0) {
            throw new IllegalArgumentException(
                    "capacity and refill rate must be positive; got " + capacity + ", " + refillPerSecond);
        }

        List<Long> result = redis.execute(
                (RedisScript<List<Long>>) (RedisScript<?>) script,
                List.of(KEY_PREFIX + apiKeyId),
                Integer.toString(capacity),
                Double.toString(refillPerSecond),
                Long.toString(System.currentTimeMillis()),
                Integer.toString(cost));

        if (result == null || result.size() < 3) {
            // Fail open on a Redis outage rather than rejecting all traffic.
            // A rate limiter is a protection, not a dependency — taking the
            // API down to enforce a quota inverts the priority. The
            // corresponding alert is in deploy/prometheus/alerts.yml.
            return new Decision(true, capacity, 0);
        }

        return new Decision(result.get(0) == 1L, result.get(1), result.get(2));
    }
}
