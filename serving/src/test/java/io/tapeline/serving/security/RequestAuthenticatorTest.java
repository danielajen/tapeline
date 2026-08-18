package io.tapeline.serving.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RequestAuthenticatorTest {

    private static final String KEY_ID = "tk_live_abc";
    private static final String SECRET = "shhh-this-is-the-secret";

    private ApiKeyRepository keys;
    private ReplayGuard replayGuard;
    private TokenBucketRateLimiter rateLimiter;
    private RequestAuthenticator authenticator;

    private final ApiKey key = new ApiKey(
            KEY_ID, SECRET, "daniel", 100, 10.0, true, Instant.EPOCH);

    private final SignedRequest request =
            SignedRequest.of("GET", "/api/v1/quotes/BTC-USD", 1_755_400_000L, "nonce-1", null);

    @BeforeEach
    void setUp() {
        keys = mock(ApiKeyRepository.class);
        replayGuard = mock(ReplayGuard.class);
        rateLimiter = mock(TokenBucketRateLimiter.class);
        authenticator = new RequestAuthenticator(keys, replayGuard, rateLimiter);

        when(keys.findByKeyId(KEY_ID)).thenReturn(Optional.of(key));
        when(replayGuard.check(anyString(), anyString(), anyLong()))
                .thenReturn(ReplayGuard.Result.OK);
        when(rateLimiter.tryConsume(anyString(), anyInt(), anyDouble(), anyInt()))
                .thenReturn(new TokenBucketRateLimiter.Decision(true, 99, 0));
    }

    private RequestAuthenticator.Credentials credentialsSignedWith(String secret) {
        String signature = new HmacSigner(secret).sign(request);
        return new RequestAuthenticator.Credentials(
                KEY_ID, signature, request.nonce(), request.timestampSeconds());
    }

    @Test
    void aCorrectlySignedRequestIsAuthenticated() {
        var outcome = authenticator.authenticate(credentialsSignedWith(SECRET), request, 1);

        assertThat(outcome.authenticated()).isTrue();
        assertThat(outcome.key().keyId()).isEqualTo(KEY_ID);
    }

    @Test
    void incompleteCredentialsAreRejectedBeforeAnyLookup() {
        var outcome = authenticator.authenticate(
                new RequestAuthenticator.Credentials(KEY_ID, null, "n", 1L), request, 1);

        assertThat(outcome.failure()).isEqualTo(RequestAuthenticator.Failure.MISSING_CREDENTIALS);
        verify(keys, never()).findByKeyId(anyString());
    }

    @Test
    void anUnknownKeyIsRejected() {
        when(keys.findByKeyId(KEY_ID)).thenReturn(Optional.empty());

        assertThat(authenticator.authenticate(credentialsSignedWith(SECRET), request, 1).failure())
                .isEqualTo(RequestAuthenticator.Failure.UNKNOWN_KEY);
    }

    @Test
    void aDisabledKeyIsRejected() {
        when(keys.findByKeyId(KEY_ID)).thenReturn(Optional.of(
                new ApiKey(KEY_ID, SECRET, "daniel", 100, 10.0, false, Instant.EPOCH)));

        assertThat(authenticator.authenticate(credentialsSignedWith(SECRET), request, 1).failure())
                .isEqualTo(RequestAuthenticator.Failure.DISABLED_KEY);
    }

    /**
     * The ordering property. A caller who cannot produce a valid signature
     * must not be able to consume a legitimate client's nonce or spend its
     * rate-limit tokens — otherwise unauthenticated traffic becomes a denial
     * of service against a real tenant.
     */
    @Test
    void aBadSignatureConsumesNeitherNonceNorTokens() {
        var outcome = authenticator.authenticate(
                credentialsSignedWith("wrong-secret"), request, 1);

        assertThat(outcome.failure()).isEqualTo(RequestAuthenticator.Failure.BAD_SIGNATURE);
        verify(replayGuard, never()).check(anyString(), anyString(), anyLong());
        verify(rateLimiter, never()).tryConsume(anyString(), anyInt(), anyDouble(), anyInt());
    }

    @Test
    void aReplayedNonceIsRejectedWithoutSpendingTokens() {
        when(replayGuard.check(anyString(), anyString(), anyLong()))
                .thenReturn(ReplayGuard.Result.NONCE_REUSED);

        var outcome = authenticator.authenticate(credentialsSignedWith(SECRET), request, 1);

        assertThat(outcome.failure()).isEqualTo(RequestAuthenticator.Failure.REPLAYED_NONCE);
        verify(rateLimiter, never()).tryConsume(anyString(), anyInt(), anyDouble(), anyInt());
    }

    @Test
    void aStaleTimestampIsRejected() {
        when(replayGuard.check(anyString(), anyString(), anyLong()))
                .thenReturn(ReplayGuard.Result.TIMESTAMP_OUT_OF_WINDOW);

        assertThat(authenticator.authenticate(credentialsSignedWith(SECRET), request, 1).failure())
                .isEqualTo(RequestAuthenticator.Failure.STALE_TIMESTAMP);
    }

    @Test
    void throttlingReportsTheRetryDelay() {
        when(rateLimiter.tryConsume(anyString(), anyInt(), anyDouble(), anyInt()))
                .thenReturn(new TokenBucketRateLimiter.Decision(false, 0, 7));

        var outcome = authenticator.authenticate(credentialsSignedWith(SECRET), request, 1);

        assertThat(outcome.authenticated()).isFalse();
        assertThat(outcome.failure()).isEqualTo(RequestAuthenticator.Failure.RATE_LIMITED);
        assertThat(outcome.retryAfterSeconds()).isEqualTo(7);
    }

    @Test
    void theRequestCostIsPassedThroughToTheLimiter() {
        authenticator.authenticate(credentialsSignedWith(SECRET), request, 10);

        // A streaming subscription must be charged more than a point read.
        verify(rateLimiter).tryConsume(KEY_ID, 100, 10.0, 10);
    }
}
