package io.tapeline.serving.quotes;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.tapeline.serving.avro.ConfluentAvroReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes the quote topic the stream tier produces, updates the hot cache,
 * and fans out to live subscribers.
 *
 * <p>The cache write comes before the broadcast. If the order were reversed, a
 * client could receive a streamed quote and then read an older one back from
 * {@code GetQuote} in the same instant — a read-your-writes violation that is
 * confusing to debug and trivially avoided.
 */
@Component
public class QuoteConsumer {

    private static final Logger log = LoggerFactory.getLogger(QuoteConsumer.class);

    private final ConfluentAvroReader avro;
    private final QuoteCache cache;
    private final QuoteBroadcaster broadcaster;
    private final Schema readerSchema;

    private final Counter consumed;
    private final Counter failed;
    private final Timer processing;

    public QuoteConsumer(
            ConfluentAvroReader avro,
            QuoteCache cache,
            QuoteBroadcaster broadcaster,
            MeterRegistry meters) {
        this.avro = avro;
        this.cache = cache;
        this.broadcaster = broadcaster;
        this.readerSchema = loadReaderSchema();

        this.consumed = Counter.builder("tapeline.serving.quotes.consumed")
                .description("Quote records consumed from Kafka")
                .register(meters);
        this.failed = Counter.builder("tapeline.serving.quotes.failed")
                .description("Quote records that could not be decoded")
                .register(meters);
        this.processing = Timer.builder("tapeline.serving.quotes.processing")
                .description("Time to decode, cache and fan out one quote")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meters);
    }

    @KafkaListener(
            topics = "${tapeline.topics.quotes}",
            groupId = "${tapeline.kafka.group-id}",
            containerFactory = "byteArrayKafkaListenerContainerFactory")
    public void onQuotes(List<ConsumerRecord<String, byte[]>> records) {
        for (ConsumerRecord<String, byte[]> record : records) {
            processing.record(() -> handle(record));
        }
    }

    private void handle(ConsumerRecord<String, byte[]> record) {
        try {
            QuoteSnapshot quote = QuoteSnapshot.fromAvro(avro.read(record.value(), readerSchema));
            cache.put(quote);
            broadcaster.publish(quote);
            consumed.increment();
        } catch (RuntimeException e) {
            // A single poison record must not stop the partition. The failure
            // counter is what turns a silent drop into an alertable signal —
            // a sustained non-zero rate here almost always means a schema
            // change that this service has not been redeployed for.
            failed.increment();
            log.warn(
                    "dropping undecodable quote at {}-{} offset {} (schema id {})",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    ConfluentAvroReader.schemaIdOf(record.value()),
                    e);
        }
    }

    /**
     * The reader schema this service compiles against. Kept as a resource so
     * it is the same file the stream tier writes with, and so a schema change
     * is a visible diff rather than an edit buried in a string literal.
     */
    private static Schema loadReaderSchema() {
        try (InputStream in = QuoteConsumer.class.getResourceAsStream("/avro/quote.v1.avsc")) {
            if (in == null) {
                throw new IllegalStateException("missing /avro/quote.v1.avsc on the classpath");
            }
            return new Schema.Parser().parse(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IllegalStateException("could not read the quote reader schema", e);
        }
    }
}
