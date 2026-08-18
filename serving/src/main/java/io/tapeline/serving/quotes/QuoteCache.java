package io.tapeline.serving.quotes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * The hot path: latest quote per (symbol, venue), in Redis.
 *
 * <p>Redis holds one hash per symbol with a field per venue, rather than one
 * key per (symbol, venue). That shape is chosen for the read pattern: the
 * common query is "every venue's quote for this symbol", which is one HGETALL
 * instead of N round trips or a pipeline.
 *
 * <p>Every hash carries a TTL. Without one, a delisted symbol's quote would
 * be served forever after the stream stopped producing it — stale data that
 * looks exactly like fresh data is worse than no data, and the freshness
 * check in {@link #latest} exists for the window before the TTL fires.
 */
@Component
public class QuoteCache {

    private static final Logger log = LoggerFactory.getLogger(QuoteCache.class);
    private static final String KEY_PREFIX = "tapeline:quote:";

    /** How long a quote survives with no updates. Long enough to ride out a
     * stream restart, short enough that a dead symbol disappears. */
    static final Duration TTL = Duration.ofMinutes(5);

    private final StringRedisTemplate redis;
    private final ObjectMapper json;

    public QuoteCache(StringRedisTemplate redis, ObjectMapper json) {
        this.redis = redis;
        this.json = json;
    }

    public void put(QuoteSnapshot quote) {
        String key = KEY_PREFIX + quote.symbol();
        try {
            redis.opsForHash().put(key, quote.venue(), json.writeValueAsString(quote));
            redis.expire(key, TTL);
        } catch (JsonProcessingException e) {
            // Serialization failure means a code bug, not a runtime condition.
            // Log it rather than failing the consumer and stalling the topic.
            log.error("could not serialize quote for {}/{}", quote.symbol(), quote.venue(), e);
        }
    }

    public Optional<QuoteSnapshot> latest(String symbol, String venue) {
        Object raw = redis.opsForHash().get(KEY_PREFIX + symbol, venue);
        return parse(raw);
    }

    /** Every venue's latest quote for a symbol, in one round trip. */
    public List<QuoteSnapshot> latestForSymbol(String symbol) {
        Map<Object, Object> entries = redis.opsForHash().entries(KEY_PREFIX + symbol);
        List<QuoteSnapshot> out = new ArrayList<>(entries.size());
        for (Object value : entries.values()) {
            parse(value).ifPresent(out::add);
        }
        return out;
    }

    /**
     * The freshest valid quote across venues, which is what an unqualified
     * "what is BTC-USD trading at" should return.
     *
     * <p>Invalid quotes are excluded rather than ranked last: a crossed book
     * is not a worse answer, it is a wrong one.
     */
    public Optional<QuoteSnapshot> best(String symbol) {
        return latestForSymbol(symbol).stream()
                .filter(QuoteSnapshot::isValid)
                .max((a, b) -> Long.compare(a.eventTimeUs(), b.eventTimeUs()));
    }

    private Optional<QuoteSnapshot> parse(Object raw) {
        if (raw == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(json.readValue(raw.toString(), QuoteSnapshot.class));
        } catch (JsonProcessingException e) {
            log.warn("dropping an unparseable cached quote", e);
            return Optional.empty();
        }
    }
}
