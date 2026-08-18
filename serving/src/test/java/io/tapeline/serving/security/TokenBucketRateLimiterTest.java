package io.tapeline.serving.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

class TokenBucketRateLimiterTest {

    @SuppressWarnings("unchecked")
    private static StringRedisTemplate redisReturning(List<Long> scriptResult) {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenReturn(scriptResult);
        return redis;
    }

    @Test
    void anAllowedRequestReportsRemainingTokens() {
        var limiter = new TokenBucketRateLimiter(redisReturning(List.of(1L, 99L, 0L)));

        var decision = limiter.tryConsume("key-1", 100, 10.0);

        assertThat(decision.allowed()).isTrue();
        assertThat(decision.remainingTokens()).isEqualTo(99);
        assertThat(decision.retryAfterSeconds()).isZero();
    }

    @Test
    void anExhaustedBucketReportsWhenToRetry() {
        var limiter = new TokenBucketRateLimiter(redisReturning(List.of(0L, 0L, 3L)));

        var decision = limiter.tryConsume("key-1", 100, 10.0);

        assertThat(decision.allowed()).isFalse();
        // A client needs to know how long to wait; without this it either
        // hammers the endpoint or backs off far longer than necessary.
        assertThat(decision.retryAfterSeconds()).isEqualTo(3);
    }

    @Test
    void redisFailureFailsOpen() {
        // A rate limiter is a protection, not a dependency. Rejecting all
        // traffic because Redis is down converts a degraded quota into a full
        // outage, which inverts the priority. The corresponding alert fires
        // on the Redis error rate instead.
        var limiter = new TokenBucketRateLimiter(redisReturning(null));

        assertThat(limiter.tryConsume("key-1", 100, 10.0).allowed()).isTrue();
    }

    @Test
    void aTruncatedScriptResultAlsoFailsOpen() {
        var limiter = new TokenBucketRateLimiter(redisReturning(List.of(1L)));
        assertThat(limiter.tryConsume("key-1", 100, 10.0).allowed()).isTrue();
    }

    @Test
    void nonsensicalLimitsAreRejectedLoudly() {
        var limiter = new TokenBucketRateLimiter(redisReturning(List.of(1L, 1L, 0L)));

        // A zero refill rate would divide by zero inside the script and a
        // zero capacity would reject everything forever. Both are bugs in the
        // key record, and they should surface here rather than in Lua.
        assertThatThrownBy(() -> limiter.tryConsume("key-1", 0, 10.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> limiter.tryConsume("key-1", 100, 0.0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
