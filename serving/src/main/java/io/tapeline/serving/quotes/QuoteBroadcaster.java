package io.tapeline.serving.quotes;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * In-process fan-out from the Kafka consumer to live gRPC streams.
 *
 * <p><b>Why fan-out lives here and not in Kafka.</b> Giving every subscriber
 * its own consumer group would make subscriber count a load multiplier on the
 * brokers, and a browser client opening a stream would provision Kafka
 * resources. One consumer per replica, fanned out in memory, means broker
 * load is a function of replicas rather than of users.
 *
 * <p><b>The cost.</b> A subscriber is bound to the replica it connected to,
 * so a rolling deploy drops every stream. Clients must reconnect — which they
 * must handle anyway, and which is why {@code StreamQuotes} opens with a
 * snapshot before it starts pushing updates.
 *
 * <p>Subscribers are keyed by symbol so a quote for BTC-USD does not walk the
 * subscriber list of every other symbol.
 */
@Component
public class QuoteBroadcaster {

    private static final Logger log = LoggerFactory.getLogger(QuoteBroadcaster.class);

    /** A live subscription. Closing it removes the listener. */
    public interface Subscription extends AutoCloseable {
        @Override
        void close();
    }

    private final Map<String, Set<Consumer<QuoteSnapshot>>> listenersBySymbol =
            new ConcurrentHashMap<>();
    private final AtomicInteger subscriberCount = new AtomicInteger();

    public QuoteBroadcaster(MeterRegistry meters) {
        meters.gauge("tapeline.serving.stream.subscribers", subscriberCount);
    }

    public Subscription subscribe(Set<String> symbols, Consumer<QuoteSnapshot> listener) {
        for (String symbol : symbols) {
            listenersBySymbol
                    .computeIfAbsent(symbol, s -> ConcurrentHashMap.newKeySet())
                    .add(listener);
        }
        subscriberCount.incrementAndGet();

        return () -> {
            for (String symbol : symbols) {
                Set<Consumer<QuoteSnapshot>> listeners = listenersBySymbol.get(symbol);
                if (listeners != null) {
                    listeners.remove(listener);
                    // Prune the empty set so a long-running process does not
                    // accumulate a map entry per symbol ever subscribed.
                    listenersBySymbol.computeIfPresent(
                            symbol, (s, set) -> set.isEmpty() ? null : set);
                }
            }
            subscriberCount.decrementAndGet();
        };
    }

    /**
     * Delivers a quote to every subscriber of its symbol.
     *
     * <p>A listener that throws must not stop delivery to the others — a
     * single client with a closed connection would otherwise blind every
     * other subscriber to that symbol.
     */
    public void publish(QuoteSnapshot quote) {
        Set<Consumer<QuoteSnapshot>> listeners = listenersBySymbol.get(quote.symbol());
        if (listeners == null || listeners.isEmpty()) {
            return;
        }
        for (Consumer<QuoteSnapshot> listener : listeners) {
            try {
                listener.accept(quote);
            } catch (RuntimeException e) {
                log.debug("dropping a quote for a failing subscriber on {}", quote.symbol(), e);
            }
        }
    }

    public int subscriberCount() {
        return subscriberCount.get();
    }

    public int symbolCount() {
        return listenersBySymbol.size();
    }
}
