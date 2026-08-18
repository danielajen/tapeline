package io.tapeline.serving.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Runs the gRPC server inside the Spring lifecycle.
 *
 * <p>About forty lines, in exchange for not adding a starter whose release
 * cadence has to stay aligned with both Spring Boot and grpc-java.
 *
 * <p>The shutdown path is the part that matters. {@code shutdown()} stops
 * accepting new calls and lets in-flight ones finish; the grace period gives
 * long-lived streaming subscriptions a chance to close cleanly instead of
 * every subscriber seeing a connection reset on every deploy. Only after the
 * grace period does it escalate to {@code shutdownNow()}.
 */
@Component
public class GrpcServerRunner implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(GrpcServerRunner.class);

    private final int port;
    private final boolean reflectionEnabled;
    private final MarketDataService service;
    private final AuthInterceptor authInterceptor;
    private final long shutdownGraceSeconds;

    private Server server;
    private volatile boolean running;

    public GrpcServerRunner(
            MarketDataService service,
            AuthInterceptor authInterceptor,
            @Value("${tapeline.grpc.port:9090}") int port,
            @Value("${tapeline.grpc.reflection-enabled:false}") boolean reflectionEnabled,
            @Value("${tapeline.grpc.shutdown-grace-seconds:20}") long shutdownGraceSeconds) {
        this.service = service;
        this.authInterceptor = authInterceptor;
        this.port = port;
        this.reflectionEnabled = reflectionEnabled;
        this.shutdownGraceSeconds = shutdownGraceSeconds;
    }

    @Override
    public void start() {
        ServerBuilder<?> builder = ServerBuilder.forPort(port)
                .addService(service)
                .intercept(authInterceptor);

        // Reflection makes grpcurl work without a proto file, which is
        // invaluable locally and an unnecessary disclosure of the API surface
        // in production. Off by default, on in the compose profile.
        if (reflectionEnabled) {
            builder.addService(ProtoReflectionServiceV1.newInstance());
        }

        try {
            server = builder.build().start();
            running = true;
            log.info("gRPC server listening on {} (reflection={})", port, reflectionEnabled);
        } catch (IOException e) {
            throw new IllegalStateException("could not start the gRPC server on port " + port, e);
        }
    }

    @Override
    public void stop() {
        if (server == null) {
            return;
        }
        running = false;
        log.info("shutting down the gRPC server, grace period {}s", shutdownGraceSeconds);
        server.shutdown();
        try {
            if (!server.awaitTermination(shutdownGraceSeconds, TimeUnit.SECONDS)) {
                log.warn("gRPC did not drain within the grace period, forcing shutdown");
                server.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            server.shutdownNow();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Start late and stop early relative to the rest of the context, so the
     * server never accepts a call before its dependencies are up. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    public int port() {
        return server == null ? port : server.getPort();
    }
}
