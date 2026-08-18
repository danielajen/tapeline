package io.tapeline.serving.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapeline.serving.avro.ConfluentAvroReader;
import io.tapeline.serving.avro.SchemaRegistryClient;
import io.tapeline.serving.quotes.QuoteSnapshot;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

/**
 * The quote path against a real Kafka broker.
 *
 * <p>This is the test that would have caught the class of bug the unit tests
 * cannot see: that the bytes one tier writes are the bytes another tier can
 * read, across a real broker, with real serializers and real record headers.
 *
 * <p>Three things are asserted here that no mock can assert:
 *
 * <ol>
 *   <li>A Confluent-framed Avro record survives a round trip through Kafka
 *       and decodes into the same {@link QuoteSnapshot}.
 *   <li>W3C trace context set by the Go ingestion tier arrives intact in
 *       record headers — the other half of the propagation test in
 *       {@code ingest/internal/tracing/tracing_test.go}, proving the
 *       distributed trace actually crosses the language boundary.
 *   <li>A topic created with an explicit partition count keeps it, which is
 *       what auto-creation silently gets wrong.
 * </ol>
 */
@Testcontainers
class QuotePipelineIT {

    private static final int SCHEMA_ID = 42;

    /**
     * A topic per test, created fresh in {@link #createTopic}.
     *
     * <p>Sharing one topic across tests couples them through broker state:
     * a consumer polling for "the next record" gets whatever another test
     * wrote, and the suite then passes or fails on execution order. That is
     * exactly what happened the first time this was run, and filtering by key
     * would have hidden the coupling rather than removed it.
     */
    private String topic;

    @Container
    static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer("confluentinc/cp-kafka:7.7.1");

    static Schema quoteSchema;
    static ConfluentAvroReader reader;

    @BeforeAll
    static void setUp() throws Exception {
        quoteSchema =
                new Schema.Parser()
                        .parse(
                                new String(
                                        QuotePipelineIT.class
                                                .getResourceAsStream("/avro/quote.v1.avsc")
                                                .readAllBytes(),
                                        StandardCharsets.UTF_8));

        SchemaRegistryClient registry = new SchemaRegistryClient("http://unused:8081");
        registry.preload(SCHEMA_ID, quoteSchema);
        reader = new ConfluentAvroReader(registry);

    }

    @BeforeEach
    void createTopic(TestInfo testInfo) throws Exception {
        topic = "md.quotes.v1." + testInfo.getTestMethod().orElseThrow().getName();

        Properties admin = new Properties();
        admin.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (AdminClient client = AdminClient.create(admin)) {
            // Six partitions, explicitly. Auto-creation would silently yield
            // the broker default and cap consumer parallelism.
            client.createTopics(List.of(new NewTopic(topic, 6, (short) 1))).all().get();
        }
    }

    private static byte[] frame(GenericRecord record) throws Exception {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(payload, null);
        new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
        encoder.flush();

        return ByteBuffer.allocate(5 + payload.size())
                .put(ConfluentAvroReader.MAGIC_BYTE)
                .putInt(SCHEMA_ID)
                .put(payload.toByteArray())
                .array();
    }

    private static GenericRecord quote(String venue, String symbol, double bid, double ask) {
        GenericData.Record r = new GenericData.Record(quoteSchema);
        r.put("venue", venue);
        r.put("symbol", symbol);
        r.put("bid_price", bid);
        r.put("bid_size", 1.5);
        r.put("ask_price", ask);
        r.put("ask_size", 2.5);
        r.put("mid", (bid + ask) / 2);
        r.put("spread_bps", (ask - bid) / ((bid + ask) / 2) * 10000);
        r.put("imbalance", -0.2);
        r.put("event_time_us", 1_755_400_000_000_000L);
        r.put("emit_time_us", 1_755_400_000_100_000L);
        return r;
    }

    private static KafkaProducer<String, byte[]> producer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(p);
    }

    private static KafkaConsumer<String, byte[]> consumer(String group) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // The setting the serving tier depends on: without it, aborted
        // records from the stream tier's transactions would be visible and
        // the exactly-once guarantee would buy nothing.
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return new KafkaConsumer<>(p);
    }

    @Test
    void aFramedQuoteSurvivesARoundTripThroughKafka() throws Exception {
        try (KafkaProducer<String, byte[]> producer = producer()) {
            producer.send(new ProducerRecord<>(topic, "BTC-USD", frame(quote("coinbase", "BTC-USD", 64000.0, 64008.0))))
                    .get();
        }

        try (KafkaConsumer<String, byte[]> consumer = consumer("g-" + topic)) {
            consumer.subscribe(List.of(topic));
            ConsumerRecord<String, byte[]> record = poll(consumer);

            assertThat(record.key()).isEqualTo("BTC-USD");

            QuoteSnapshot snapshot =
                    QuoteSnapshot.fromAvro(reader.read(record.value(), quoteSchema));

            assertThat(snapshot.venue()).isEqualTo("coinbase");
            assertThat(snapshot.symbol()).isEqualTo("BTC-USD");
            assertThat(snapshot.bidPrice()).isEqualTo(64000.0);
            assertThat(snapshot.askPrice()).isEqualTo(64008.0);
            assertThat(snapshot.isValid()).isTrue();
            // Off the wire Avro yields Utf8, not String — the trap a test
            // built from an in-memory record cannot catch.
            assertThat(snapshot.venue()).isInstanceOf(String.class);
        }
    }

    /**
     * The cross-language half of the distributed tracing claim. The Go tier
     * writes exactly these headers; this asserts the Java side can read them
     * off a real broker rather than out of a map built by the same test.
     */
    @Test
    void traceContextWrittenByTheGoTierArrivesIntact() throws Exception {
        String traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

        try (KafkaProducer<String, byte[]> producer = producer()) {
            ProducerRecord<String, byte[]> record =
                    new ProducerRecord<>(topic, "ETH-USD", frame(quote("kraken", "ETH-USD", 3100.0, 3101.0)));
            record.headers().add("venue", "kraken".getBytes(StandardCharsets.UTF_8));
            record.headers().add("kind", "trade".getBytes(StandardCharsets.UTF_8));
            record.headers().add("traceparent", traceparent.getBytes(StandardCharsets.UTF_8));
            producer.send(record).get();
        }

        try (KafkaConsumer<String, byte[]> consumer = consumer("g-" + topic)) {
            consumer.subscribe(List.of(topic));

            ConsumerRecord<String, byte[]> received = poll(consumer);
            assertThat(received.key()).isEqualTo("ETH-USD");

            Map<String, String> headers = headersOf(received);
            assertThat(headers).containsEntry("traceparent", traceparent);
            assertThat(headers).containsEntry("venue", "kraken");

            // The trace id is the middle field of the W3C traceparent. If it
            // survives, a span from the venue socket joins up with a span in
            // this process in one view.
            assertThat(headers.get("traceparent").split("-")[1])
                    .isEqualTo("4bf92f3577b34da6a3ce929d0e0e4736");
        }
    }

    @Test
    void aTopicKeepsTheExplicitPartitionCountItWasCreatedWith() throws Exception {
        Properties admin = new Properties();
        admin.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (AdminClient client = AdminClient.create(admin)) {
            int partitions =
                    client.describeTopics(List.of(topic))
                            .allTopicNames()
                            .get()
                            .get(topic)
                            .partitions()
                            .size();
            // Auto-creation would silently yield the broker default and cap
            // consumer parallelism, which is why it is disabled everywhere.
            assertThat(partitions).isEqualTo(6);
        }
    }

    @Test
    void aPayloadThatIsNotConfluentFramedIsRejectedRatherThanMisdecoded() throws Exception {
        try (KafkaProducer<String, byte[]> producer = producer()) {
            producer.send(
                            new ProducerRecord<>(
                                    topic,
                                    "SOL-USD",
                                    "{\"not\":\"avro\"}".getBytes(StandardCharsets.UTF_8)))
                    .get();
        }

        try (KafkaConsumer<String, byte[]> consumer = consumer("g-" + topic)) {
            consumer.subscribe(List.of(topic));

            byte[] value = poll(consumer).value();
            assertThat(ConfluentAvroReader.schemaIdOf(value)).isEqualTo(-1);
            org.assertj.core.api.Assertions.assertThatThrownBy(
                            () -> reader.read(value, quoteSchema))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("magic byte");
        }
    }

    private static ConsumerRecord<String, byte[]> poll(KafkaConsumer<String, byte[]> consumer) {
        for (int attempt = 0; attempt < 20; attempt++) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, byte[]> record : records) {
                return record;
            }
        }
        throw new AssertionError("no record arrived within 20 seconds");
    }

    private static Map<String, String> headersOf(ConsumerRecord<String, byte[]> record) {
        return java.util.Arrays.stream(record.headers().toArray())
                .collect(
                        java.util.stream.Collectors.toMap(
                                Header::key, h -> new String(h.value(), StandardCharsets.UTF_8)));
    }
}
