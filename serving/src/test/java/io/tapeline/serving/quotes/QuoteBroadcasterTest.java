package io.tapeline.serving.quotes;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QuoteBroadcasterTest {

    private QuoteBroadcaster broadcaster;

    @BeforeEach
    void setUp() {
        broadcaster = new QuoteBroadcaster(new SimpleMeterRegistry());
    }

    private static QuoteSnapshot quote(String symbol) {
        return new QuoteSnapshot(
                "coinbase", symbol, 100.0, 1.0, 101.0, 1.0, 100.5, 99.5, 0.0, 1L, 2L);
    }

    @Test
    void aSubscriberReceivesQuotesForItsSymbols() {
        List<QuoteSnapshot> received = new CopyOnWriteArrayList<>();
        broadcaster.subscribe(Set.of("BTC-USD"), received::add);

        broadcaster.publish(quote("BTC-USD"));

        assertThat(received).hasSize(1);
        assertThat(received.get(0).symbol()).isEqualTo("BTC-USD");
    }

    @Test
    void quotesAreNotDeliveredToSubscribersOfOtherSymbols() {
        List<QuoteSnapshot> btc = new CopyOnWriteArrayList<>();
        List<QuoteSnapshot> eth = new CopyOnWriteArrayList<>();
        broadcaster.subscribe(Set.of("BTC-USD"), btc::add);
        broadcaster.subscribe(Set.of("ETH-USD"), eth::add);

        broadcaster.publish(quote("BTC-USD"));

        assertThat(btc).hasSize(1);
        assertThat(eth).isEmpty();
    }

    @Test
    void everySubscriberOfASymbolReceivesEachQuote() {
        AtomicInteger a = new AtomicInteger();
        AtomicInteger b = new AtomicInteger();
        broadcaster.subscribe(Set.of("BTC-USD"), q -> a.incrementAndGet());
        broadcaster.subscribe(Set.of("BTC-USD"), q -> b.incrementAndGet());

        broadcaster.publish(quote("BTC-USD"));

        assertThat(a.get()).isEqualTo(1);
        assertThat(b.get()).isEqualTo(1);
    }

    @Test
    void closingASubscriptionStopsDeliveryAndReleasesTheSymbolEntry() {
        AtomicInteger count = new AtomicInteger();
        var subscription = broadcaster.subscribe(Set.of("BTC-USD"), q -> count.incrementAndGet());

        broadcaster.publish(quote("BTC-USD"));
        subscription.close();
        broadcaster.publish(quote("BTC-USD"));

        assertThat(count.get()).isEqualTo(1);
        assertThat(broadcaster.subscriberCount()).isZero();
        // Pruning the empty set matters: without it a long-running process
        // accumulates a map entry for every symbol ever subscribed.
        assertThat(broadcaster.symbolCount()).isZero();
    }

    /**
     * One client with a dead connection must not blind every other subscriber
     * to the same symbol — which is what an unguarded loop over listeners
     * would do the first time onNext threw.
     */
    @Test
    void aThrowingSubscriberDoesNotStopDeliveryToOthers() {
        AtomicInteger healthy = new AtomicInteger();
        broadcaster.subscribe(Set.of("BTC-USD"), q -> {
            throw new IllegalStateException("connection closed");
        });
        broadcaster.subscribe(Set.of("BTC-USD"), q -> healthy.incrementAndGet());

        broadcaster.publish(quote("BTC-USD"));

        assertThat(healthy.get()).isEqualTo(1);
    }

    @Test
    void publishingWithNoSubscribersIsANoOp() {
        broadcaster.publish(quote("BTC-USD"));
        assertThat(broadcaster.subscriberCount()).isZero();
    }

    @Test
    void aSubscriptionCanSpanSeveralSymbols() {
        List<QuoteSnapshot> received = new CopyOnWriteArrayList<>();
        var subscription =
                broadcaster.subscribe(Set.of("BTC-USD", "ETH-USD", "SOL-USD"), received::add);

        broadcaster.publish(quote("BTC-USD"));
        broadcaster.publish(quote("ETH-USD"));
        broadcaster.publish(quote("SOL-USD"));

        assertThat(received).hasSize(3);

        subscription.close();
        assertThat(broadcaster.symbolCount()).isZero();
    }

    @Test
    void concurrentPublishAndSubscribeIsSafe() throws Exception {
        int threads = 8;
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger errors = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            final int id = i;
            new Thread(() -> {
                try {
                    for (int n = 0; n < 200; n++) {
                        var sub = broadcaster.subscribe(Set.of("BTC-USD"), q -> {});
                        broadcaster.publish(quote("BTC-USD"));
                        sub.close();
                    }
                } catch (RuntimeException e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            }, "pub-sub-" + id).start();
        }

        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(errors.get()).isZero();
        assertThat(broadcaster.subscriberCount()).isZero();
    }
}
