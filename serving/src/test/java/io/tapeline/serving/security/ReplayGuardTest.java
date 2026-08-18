package io.tapeline.serving.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

class ReplayGuardTest {

    private static final long NOW_SECONDS = 1_755_400_000L;
    private static final Duration SKEW = Duration.ofSeconds(30);

    private StringRedisTemplate redis;
    private ValueOperations<String, String> valueOps;
    private ReplayGuard guard;

    /** Stands in for Redis SETNX: the first claim of a key wins. */
    private Set<String> claimed;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redis = mock(StringRedisTemplate.class);
        valueOps = mock(ValueOperations.class);
        claimed = new HashSet<>();

        when(redis.opsForValue()).thenReturn(valueOps);
        when(valueOps.setIfAbsent(anyString(), anyString(), anyLong(), any(TimeUnit.class)))
                .thenAnswer(invocation -> claimed.add(invocation.getArgument(0)));

        Clock fixed = Clock.fixed(Instant.ofEpochSecond(NOW_SECONDS), ZoneOffset.UTC);
        guard = new ReplayGuard(redis, fixed, SKEW);
    }

    @Test
    void aFreshNonceInsideTheWindowIsAccepted() {
        assertThat(guard.check("key-1", "nonce-a", NOW_SECONDS))
                .isEqualTo(ReplayGuard.Result.OK);
    }

    @Test
    void reusingANonceIsRejected() {
        assertThat(guard.check("key-1", "nonce-a", NOW_SECONDS))
                .isEqualTo(ReplayGuard.Result.OK);
        assertThat(guard.check("key-1", "nonce-a", NOW_SECONDS))
                .isEqualTo(ReplayGuard.Result.NONCE_REUSED);
    }

    @Test
    void nonceSpaceIsNamespacedPerKey() {
        // Otherwise one tenant could deny another simply by burning nonce
        // values that the other happens to choose.
        assertThat(guard.check("key-1", "shared-nonce", NOW_SECONDS))
                .isEqualTo(ReplayGuard.Result.OK);
        assertThat(guard.check("key-2", "shared-nonce", NOW_SECONDS))
                .isEqualTo(ReplayGuard.Result.OK);
    }

    @Test
    void timestampsOutsideTheWindowAreRejectedInBothDirections() {
        assertThat(guard.check("key-1", "n1", NOW_SECONDS - SKEW.toSeconds() - 1))
                .as("too old")
                .isEqualTo(ReplayGuard.Result.TIMESTAMP_OUT_OF_WINDOW);

        // Future timestamps matter too: without this check a client could mint
        // a request dated far ahead and replay it much later.
        assertThat(guard.check("key-1", "n2", NOW_SECONDS + SKEW.toSeconds() + 1))
                .as("too far ahead")
                .isEqualTo(ReplayGuard.Result.TIMESTAMP_OUT_OF_WINDOW);
    }

    @Test
    void theWindowEdgesAreInclusive() {
        assertThat(guard.check("key-1", "n1", NOW_SECONDS - SKEW.toSeconds()))
                .isEqualTo(ReplayGuard.Result.OK);
        assertThat(guard.check("key-1", "n2", NOW_SECONDS + SKEW.toSeconds()))
                .isEqualTo(ReplayGuard.Result.OK);
    }

    @Test
    void aStaleTimestampIsRejectedWithoutConsumingANonce() {
        guard.check("key-1", "n1", NOW_SECONDS - 10_000);
        // An unauthenticated caller must not be able to burn nonce values.
        assertThat(claimed).isEmpty();
    }

    @Test
    void theNonceTtlCoversTheWholeWindow() {
        guard.check("key-1", "n1", NOW_SECONDS);

        ArgumentCaptor<Long> ttl = ArgumentCaptor.forClass(Long.class);
        verify(valueOps).setIfAbsent(anyString(), anyString(), ttl.capture(), eq(TimeUnit.SECONDS));

        // A TTL shorter than the window would let a request expire from the
        // nonce set while its timestamp is still acceptable — reopening the
        // exact hole the nonce exists to close.
        assertThat(ttl.getValue()).isGreaterThanOrEqualTo(SKEW.toSeconds() * 2);
    }
}
