package io.tapeline.serving.security;

import java.time.Instant;

/**
 * An issued API key.
 *
 * <p>The secret is stored in Postgres alongside the key id because HMAC
 * requires the server to hold the same secret the client signs with — unlike
 * a password, it cannot be stored as a one-way hash. That is a real property
 * of the scheme and worth stating rather than glossing: the mitigation is
 * encryption at rest and tight access to the table, not a hash.
 */
public record ApiKey(
        String keyId,
        String secret,
        String owner,
        int rateLimitCapacity,
        double rateLimitRefillPerSecond,
        boolean enabled,
        Instant createdAt) {

    public boolean isUsable() {
        return enabled && secret != null && !secret.isBlank();
    }
}
