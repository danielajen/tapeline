package io.tapeline.serving.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.tapeline.serving.security.ApiKey;
import io.tapeline.serving.security.RequestAuthenticator;
import io.tapeline.serving.security.SignedRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Applies the same HMAC scheme to gRPC that the REST gateway applies to HTTP.
 *
 * <p>One authenticator, two transports. The canonical string for a gRPC call
 * uses {@code POST} and the full method name as the path, so a signature is
 * still bound to the operation being invoked and cannot be lifted from one
 * RPC to another.
 *
 * <p>There is no body hash: gRPC request messages arrive after the headers,
 * so signing the payload would mean buffering the message before
 * authenticating it — which lets an unauthenticated caller make the server
 * allocate. The empty-body hash is used instead, and the tradeoff is stated
 * rather than hidden: the method, timestamp and nonce are bound, the message
 * body is not.
 */
@Component
public class AuthInterceptor implements ServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(AuthInterceptor.class);

    public static final Metadata.Key<String> KEY_ID =
            Metadata.Key.of("x-tapeline-key", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> SIGNATURE =
            Metadata.Key.of("x-tapeline-signature", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> TIMESTAMP =
            Metadata.Key.of("x-tapeline-timestamp", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> NONCE =
            Metadata.Key.of("x-tapeline-nonce", Metadata.ASCII_STRING_MARSHALLER);

    /** The authenticated key, available to service code for per-tenant logic. */
    public static final Context.Key<ApiKey> API_KEY_CONTEXT = Context.key("tapeline-api-key");

    /** A streaming subscription costs more than a point read, because it
     * consumes server resources for as long as it lives. */
    private static final int STREAMING_COST = 10;
    private static final int UNARY_COST = 1;

    private final RequestAuthenticator authenticator;

    public AuthInterceptor(RequestAuthenticator authenticator) {
        this.authenticator = authenticator;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String method = call.getMethodDescriptor().getFullMethodName();

        long timestamp = parseTimestamp(headers.get(TIMESTAMP));
        var credentials = new RequestAuthenticator.Credentials(
                headers.get(KEY_ID), headers.get(SIGNATURE), headers.get(NONCE), timestamp);

        SignedRequest signed = new SignedRequest(
                "POST", "/" + method, timestamp,
                credentials.nonce() == null ? "-" : credentials.nonce(),
                SignedRequest.EMPTY_BODY_SHA256);

        int cost = call.getMethodDescriptor().getType().serverSendsOneMessage()
                ? UNARY_COST
                : STREAMING_COST;

        RequestAuthenticator.Outcome outcome;
        try {
            outcome = authenticator.authenticate(credentials, signed, cost);
        } catch (IllegalArgumentException e) {
            // A malformed credential set (blank nonce, for instance) reaches
            // SignedRequest's constructor checks. Treat it as unauthenticated,
            // not as a server error.
            outcome = RequestAuthenticator.Outcome.denied(
                    RequestAuthenticator.Failure.MISSING_CREDENTIALS);
        }

        if (!outcome.authenticated()) {
            // One status for every authentication failure, with the specific
            // reason only in the server log. Distinguishing "unknown key" from
            // "bad signature" to the caller is an enumeration oracle.
            Status status = outcome.failure() == RequestAuthenticator.Failure.RATE_LIMITED
                    ? Status.RESOURCE_EXHAUSTED.withDescription(
                            "rate limited; retry after " + outcome.retryAfterSeconds() + "s")
                    : Status.UNAUTHENTICATED.withDescription("authentication failed");

            log.debug("rejecting {}: {}", method, outcome.failure());
            call.close(status, new Metadata());
            return new ServerCall.Listener<>() {};
        }

        Context context = Context.current().withValue(API_KEY_CONTEXT, outcome.key());
        return Contexts.interceptCall(context, call, headers, next);
    }

    private static long parseTimestamp(String raw) {
        if (raw == null || raw.isBlank()) {
            return 0L;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
