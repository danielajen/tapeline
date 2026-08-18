package io.tapeline.serving.web;

import io.tapeline.serving.quotes.QuoteCache;
import io.tapeline.serving.quotes.QuoteSnapshot;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * A thin REST gateway over the same data gRPC serves.
 *
 * <p>It exists because browsers cannot speak gRPC without a proxy, and
 * because {@code curl} is how anyone actually explores an API for the first
 * time. It is deliberately read-only and deliberately unary: streaming over
 * HTTP would mean SSE or WebSockets, which is a second streaming
 * implementation to keep correct for no gain over the gRPC one.
 */
@RestController
@RequestMapping("/api/v1")
public class MarketDataController {

    private final QuoteCache cache;

    public MarketDataController(QuoteCache cache) {
        this.cache = cache;
    }

    @GetMapping("/quotes/{symbol}")
    public ResponseEntity<?> quote(
            @PathVariable String symbol,
            @RequestParam(required = false) String venue) {

        var found = (venue == null || venue.isBlank())
                ? cache.best(symbol)
                : cache.latest(symbol, venue);

        return found.<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("error", "no quote for " + symbol)));
    }

    /** Every venue's quote for a symbol, which is the view that makes a
     * divergence alert interpretable. */
    @GetMapping("/quotes/{symbol}/venues")
    public ResponseEntity<List<QuoteSnapshot>> byVenue(@PathVariable String symbol) {
        List<QuoteSnapshot> quotes = cache.latestForSymbol(symbol);
        return quotes.isEmpty()
                ? ResponseEntity.notFound().build()
                : ResponseEntity.ok(quotes);
    }
}
