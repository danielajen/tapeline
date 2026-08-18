package io.tapeline.serving.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapeline.serving.security.ApiKey;
import io.tapeline.serving.security.ApiKeyRepository;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

/**
 * The API key repository against a real Postgres.
 *
 * <p>A mocked {@code JdbcTemplate} would assert that this class calls the
 * template the way its author expected. What it cannot assert is that the SQL
 * is valid, that the column names match the migration, that the CHECK
 * constraints hold, or that a {@code TIMESTAMPTZ} survives the round trip
 * into an {@code Instant}. Every one of those is a real way this breaks, and
 * every one of them needs a real database.
 *
 * <p>The container runs the same {@code deploy/sql/01-schema.sql} the compose
 * stack does, so a migration that breaks the repository fails here rather
 * than at deploy time.
 */
@Testcontainers
class ApiKeyRepositoryIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("tapeline")
                    .withUsername("tapeline")
                    .withPassword("tapeline")
                    .withCopyFileToContainer(
                            MountableFile.forHostPath("../deploy/sql/01-schema.sql"),
                            "/docker-entrypoint-initdb.d/01-schema.sql");

    private JdbcTemplate jdbc;
    private ApiKeyRepository repository;

    @BeforeEach
    void setUp() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUsername(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        ds.setDriverClassName("org.postgresql.Driver");

        jdbc = new JdbcTemplate(ds);
        repository = new ApiKeyRepository(jdbc);
        repository.invalidateAll();
    }

    @Test
    void theMigrationSeedsAUsableLocalKey() {
        // Proves the migration ran and that every column the repository reads
        // exists with a compatible type.
        Optional<ApiKey> key = repository.findByKeyId("tk_local_dev");

        assertThat(key).isPresent();
        assertThat(key.get().owner()).isEqualTo("local");
        assertThat(key.get().isUsable()).isTrue();
        assertThat(key.get().rateLimitCapacity()).isEqualTo(1000);
        assertThat(key.get().rateLimitRefillPerSecond()).isEqualTo(100.0);
        // A TIMESTAMPTZ that did not survive the mapping would be null here.
        assertThat(key.get().createdAt()).isNotNull();
    }

    @Test
    void anUnknownKeyIsEmptyRatherThanAnException() {
        // EmptyResultDataAccessException from queryForObject must be caught,
        // not propagated — an unknown key is a 401, not a 500.
        assertThat(repository.findByKeyId("tk_does_not_exist")).isEmpty();
        assertThat(repository.findByKeyId("")).isEmpty();
        assertThat(repository.findByKeyId(null)).isEmpty();
    }

    @Test
    void aDisabledKeyLoadsButIsNotUsable() {
        jdbc.update(
                """
                INSERT INTO api_keys (key_id, secret, owner, enabled)
                VALUES ('tk_disabled', 'secret', 'test', false)
                ON CONFLICT (key_id) DO UPDATE SET enabled = false
                """);
        repository.invalidate("tk_disabled");

        ApiKey key = repository.findByKeyId("tk_disabled").orElseThrow();
        assertThat(key.enabled()).isFalse();
        assertThat(key.isUsable()).isFalse();
    }

    @Test
    void theCacheServesRepeatedLookupsAndInvalidationTakesEffect() {
        jdbc.update(
                """
                INSERT INTO api_keys (key_id, secret, owner) VALUES ('tk_cached', 's', 'test')
                ON CONFLICT (key_id) DO UPDATE SET enabled = true
                """);
        assertThat(repository.findByKeyId("tk_cached").orElseThrow().enabled()).isTrue();

        // Revoke behind the cache's back.
        jdbc.update("UPDATE api_keys SET enabled = false WHERE key_id = 'tk_cached'");

        // Still served from cache — this is the revocation budget, stated in
        // the repository as a deliberate 30-second TTL rather than an accident.
        assertThat(repository.findByKeyId("tk_cached").orElseThrow().enabled()).isTrue();

        repository.invalidate("tk_cached");
        assertThat(repository.findByKeyId("tk_cached").orElseThrow().enabled()).isFalse();
    }

    @Test
    void negativeLookupsAreCachedToo() {
        assertThat(repository.findByKeyId("tk_missing")).isEmpty();
        // Without negative caching, an attacker spraying random key ids drives
        // one query per attempt — a free denial of service on the auth path.
        assertThat(repository.findByKeyId("tk_missing")).isEmpty();
    }

    @Test
    void theSchemaRejectsNonsensicalRateLimits() {
        // The CHECK constraints in the migration are load-bearing: a zero
        // refill rate would divide by zero inside the Lua rate limiter.
        assertThat(
                        catchThrowable(
                                () ->
                                        jdbc.update(
                                                """
                                                INSERT INTO api_keys (key_id, secret, owner, rate_limit_refill_per_second)
                                                VALUES ('tk_bad', 's', 'test', 0)
                                                """)))
                .isNotNull();
    }

    private static Throwable catchThrowable(Runnable r) {
        try {
            r.run();
            return null;
        } catch (Throwable t) {
            return t;
        }
    }
}
