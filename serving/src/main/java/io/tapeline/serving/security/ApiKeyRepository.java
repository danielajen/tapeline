package io.tapeline.serving.security;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/**
 * API key lookup, backed by Postgres with a short in-process cache.
 *
 * <p>The cache exists because authentication is on the path of every single
 * request, including every message of a stream subscription. A database round
 * trip per request would make Postgres the latency floor of the whole API for
 * data that changes perhaps weekly.
 *
 * <p>The TTL is thirty seconds, and that number is a revocation budget rather
 * than a performance one: it is the longest a disabled key can keep working.
 * Anything materially longer turns "revoke this key" into a support ticket.
 */
@Repository
public class ApiKeyRepository {

    private static final Duration CACHE_TTL = Duration.ofSeconds(30);

    private static final String SELECT_SQL =
            """
            SELECT key_id, secret, owner, rate_limit_capacity,
                   rate_limit_refill_per_second, enabled, created_at
            FROM api_keys
            WHERE key_id = ?
            """;

    private record CacheEntry(Optional<ApiKey> value, long expiresAtMillis) {
        boolean isFresh(long now) {
            return now < expiresAtMillis;
        }
    }

    private final JdbcTemplate jdbc;
    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();

    private static final RowMapper<ApiKey> MAPPER = (rs, rowNum) -> new ApiKey(
            rs.getString("key_id"),
            rs.getString("secret"),
            rs.getString("owner"),
            rs.getInt("rate_limit_capacity"),
            rs.getDouble("rate_limit_refill_per_second"),
            rs.getBoolean("enabled"),
            rs.getTimestamp("created_at").toInstant());

    public ApiKeyRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<ApiKey> findByKeyId(String keyId) {
        if (keyId == null || keyId.isBlank()) {
            return Optional.empty();
        }

        long now = System.currentTimeMillis();
        CacheEntry cached = cache.get(keyId);
        if (cached != null && cached.isFresh(now)) {
            return cached.value();
        }

        Optional<ApiKey> loaded;
        try {
            loaded = Optional.ofNullable(jdbc.queryForObject(SELECT_SQL, MAPPER, keyId));
        } catch (EmptyResultDataAccessException e) {
            loaded = Optional.empty();
        }

        // Negative results are cached too. Without that, an attacker spraying
        // random key ids drives one database query per attempt, which is a
        // free denial-of-service against the authentication path.
        cache.put(keyId, new CacheEntry(loaded, now + CACHE_TTL.toMillis()));
        return loaded;
    }

    /** Drops a key from the cache, so a revocation can take effect at once
     * rather than at the end of the TTL. */
    public void invalidate(String keyId) {
        cache.remove(keyId);
    }

    public void invalidateAll() {
        cache.clear();
    }
}
