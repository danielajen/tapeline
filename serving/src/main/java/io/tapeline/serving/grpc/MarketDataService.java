package io.tapeline.serving.grpc;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.tapeline.proto.v1.GetQuoteRequest;
import io.tapeline.proto.v1.MarketDataGrpc;
import io.tapeline.proto.v1.QueryWindowsRequest;
import io.tapeline.proto.v1.QueryWindowsResponse;
import io.tapeline.proto.v1.Quote;
import io.tapeline.proto.v1.StreamQuotesRequest;
import io.tapeline.serving.olap.WindowQueryService;
import io.tapeline.serving.quotes.QuoteBroadcaster;
import io.tapeline.serving.quotes.QuoteCache;
import io.tapeline.serving.quotes.QuoteSnapshot;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** The gRPC surface. */
@Component
public class MarketDataService extends MarketDataGrpc.MarketDataImplBase {

    private static final Logger log = LoggerFactory.getLogger(MarketDataService.class);

    /** Refuse absurd subscriptions rather than letting one client pin a
     * thread's worth of fan-out work. */
    private static final int MAX_SYMBOLS_PER_STREAM = 64;

    private static final int DEFAULT_UPDATES_HZ = 10;
    private static final int MAX_UPDATES_HZ = 100;

    private final QuoteCache cache;
    private final QuoteBroadcaster broadcaster;
    private final WindowQueryService windows;
    private final Counter droppedForSlowClients;

    public MarketDataService(
            QuoteCache cache,
            QuoteBroadcaster broadcaster,
            WindowQueryService windows,
            MeterRegistry meters) {
        this.cache = cache;
        this.broadcaster = broadcaster;
        this.windows = windows;
        this.droppedForSlowClients = Counter.builder("tapeline.serving.stream.dropped")
                .description("Quote updates dropped because a subscriber was not ready")
                .register(meters);
    }

    @Override
    public void getQuote(GetQuoteRequest request, StreamObserver<Quote> observer) {
        if (request.getSymbol().isBlank()) {
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription("symbol is required")
                    .asRuntimeException());
            return;
        }

        var found = request.getVenue().isBlank()
                ? cache.best(request.getSymbol())
                : cache.latest(request.getSymbol(), request.getVenue());

        if (found.isEmpty()) {
            observer.onError(Status.NOT_FOUND
                    .withDescription("no quote for " + request.getSymbol())
                    .asRuntimeException());
            return;
        }

        observer.onNext(toProto(found.get()));
        observer.onCompleted();
    }

    @Override
    public void streamQuotes(StreamQuotesRequest request, StreamObserver<Quote> observer) {
        Set<String> symbols = new LinkedHashSet<>(request.getSymbolsList());
        if (symbols.isEmpty()) {
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription("at least one symbol is required")
                    .asRuntimeException());
            return;
        }
        if (symbols.size() > MAX_SYMBOLS_PER_STREAM) {
            observer.onError(Status.RESOURCE_EXHAUSTED
                    .withDescription("at most " + MAX_SYMBOLS_PER_STREAM + " symbols per stream")
                    .asRuntimeException());
            return;
        }

        Set<String> venues = new LinkedHashSet<>(request.getVenuesList());
        int hz = request.getMaxUpdatesHz() == 0
                ? DEFAULT_UPDATES_HZ
                : Math.min(request.getMaxUpdatesHz(), MAX_UPDATES_HZ);
        long minIntervalUs = 1_000_000L / hz;

        var call = (ServerCallStreamObserver<Quote>) observer;
        AtomicLong lastSentUs = new AtomicLong(0);

        // Open with a snapshot so a subscriber has prices immediately rather
        // than waiting for the next tick — which on a quiet symbol could be
        // minutes, and which is also what makes reconnecting cheap for the
        // client after a rolling deploy.
        for (String symbol : symbols) {
            for (QuoteSnapshot snapshot : cache.latestForSymbol(symbol)) {
                if (matchesVenue(snapshot, venues) && call.isReady()) {
                    call.onNext(toProto(snapshot));
                }
            }
        }

        QuoteBroadcaster.Subscription subscription = broadcaster.subscribe(symbols, quote -> {
            if (!matchesVenue(quote, venues)) {
                return;
            }

            // Back-pressure. gRPC buffers unboundedly if you keep calling
            // onNext on a client that is not reading, and the failure mode is
            // the server running out of heap because of one slow consumer.
            // Dropping is the right call for market data: the next quote
            // supersedes this one, so a subscriber that cannot keep up should
            // get the freshest price, not a growing backlog of stale ones.
            if (!call.isReady()) {
                droppedForSlowClients.increment();
                return;
            }

            long now = quote.emitTimeUs();
            long previous = lastSentUs.get();
            if (now - previous < minIntervalUs) {
                return;
            }
            if (!lastSentUs.compareAndSet(previous, now)) {
                return;
            }

            try {
                call.onNext(toProto(quote));
            } catch (RuntimeException e) {
                log.debug("stream closed while sending", e);
            }
        });

        // Unsubscribe on every terminal path, or the broadcaster leaks a
        // listener for every client that ever connected.
        call.setOnCancelHandler(subscription::close);
        call.setOnCloseHandler(subscription::close);
    }

    @Override
    public void queryWindows(
            QueryWindowsRequest request, StreamObserver<QueryWindowsResponse> observer) {
        if (request.getSymbol().isBlank()) {
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription("symbol is required")
                    .asRuntimeException());
            return;
        }
        if (request.getEndUs() <= request.getStartUs()) {
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription("end_us must be after start_us")
                    .asRuntimeException());
            return;
        }

        try {
            observer.onNext(windows.query(request));
            observer.onCompleted();
        } catch (RuntimeException e) {
            log.error("window query failed for {}", request.getSymbol(), e);
            observer.onError(Status.INTERNAL
                    .withDescription("query failed")
                    .asRuntimeException());
        }
    }

    private static boolean matchesVenue(QuoteSnapshot quote, Set<String> venues) {
        return venues.isEmpty() || venues.contains(quote.venue());
    }

    static Quote toProto(QuoteSnapshot s) {
        return Quote.newBuilder()
                .setVenue(s.venue())
                .setSymbol(s.symbol())
                .setBidPrice(s.bidPrice())
                .setBidSize(s.bidSize())
                .setAskPrice(s.askPrice())
                .setAskSize(s.askSize())
                .setMid(s.mid())
                .setSpreadBps(s.spreadBps())
                .setImbalance(s.imbalance())
                .setEventTimeUs(s.eventTimeUs())
                .setEmitTimeUs(s.emitTimeUs())
                .build();
    }
}
