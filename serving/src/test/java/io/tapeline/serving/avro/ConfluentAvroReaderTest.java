package io.tapeline.serving.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapeline.serving.quotes.QuoteSnapshot;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConfluentAvroReaderTest {

    private static final int SCHEMA_ID = 77;

    private Schema quoteSchema;
    private SchemaRegistryClient registry;
    private ConfluentAvroReader reader;

    @BeforeEach
    void setUp() throws IOException {
        quoteSchema = loadSchema("/avro/quote.v1.avsc");
        // No registry is contacted: the id is preloaded, which is also how
        // the local compose profile runs.
        registry = new SchemaRegistryClient("http://localhost:8081");
        registry.preload(SCHEMA_ID, quoteSchema);
        reader = new ConfluentAvroReader(registry);
    }

    private static Schema loadSchema(String resource) throws IOException {
        try (InputStream in = ConfluentAvroReaderTest.class.getResourceAsStream(resource)) {
            assertThat(in).as("resource %s", resource).isNotNull();
            return new Schema.Parser()
                    .parse(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    /** Frames a record exactly as the Go and Scala producers do. */
    private static byte[] frame(int schemaId, GenericRecord record) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(payload, null);
        new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
        encoder.flush();

        ByteBuffer buffer = ByteBuffer.allocate(5 + payload.size());
        buffer.put(ConfluentAvroReader.MAGIC_BYTE);
        buffer.putInt(schemaId);
        buffer.put(payload.toByteArray());
        return buffer.array();
    }

    private GenericRecord sampleQuote() {
        GenericData.Record r = new GenericData.Record(quoteSchema);
        r.put("venue", "coinbase");
        r.put("symbol", "BTC-USD");
        r.put("bid_price", 64000.0);
        r.put("bid_size", 1.5);
        r.put("ask_price", 64008.0);
        r.put("ask_size", 2.5);
        r.put("mid", 64004.0);
        r.put("spread_bps", 1.25);
        r.put("imbalance", -0.2);
        r.put("event_time_us", 1_755_400_000_000_000L);
        r.put("emit_time_us", 1_755_400_000_100_000L);
        return r;
    }

    @Test
    void aFramedRecordDecodes() throws IOException {
        GenericRecord decoded = reader.read(frame(SCHEMA_ID, sampleQuote()), quoteSchema);
        QuoteSnapshot quote = QuoteSnapshot.fromAvro(decoded);

        assertThat(quote.venue()).isEqualTo("coinbase");
        assertThat(quote.symbol()).isEqualTo("BTC-USD");
        assertThat(quote.bidPrice()).isEqualTo(64000.0);
        assertThat(quote.askSize()).isEqualTo(2.5);
        assertThat(quote.eventTimeUs()).isEqualTo(1_755_400_000_000_000L);
        assertThat(quote.isValid()).isTrue();
    }

    @Test
    void avroStringsBecomeJavaStrings() throws IOException {
        // Off the wire Avro yields Utf8, not String. A cast would compile,
        // pass a test built from an in-memory record, and fail here.
        QuoteSnapshot quote =
                QuoteSnapshot.fromAvro(reader.read(frame(SCHEMA_ID, sampleQuote()), quoteSchema));

        assertThat(quote.venue()).isInstanceOf(String.class).isEqualTo("coinbase");
    }

    @Test
    void theSchemaIdIsReadableWithoutDecoding() throws IOException {
        assertThat(ConfluentAvroReader.schemaIdOf(frame(SCHEMA_ID, sampleQuote())))
                .isEqualTo(SCHEMA_ID);
    }

    @Test
    void aPayloadThatIsNotConfluentFramedIsRejectedClearly() {
        // 0x01 is what plain JSON or a Protobuf payload looks like here.
        // Catching it at the boundary is the difference between a clear error
        // and a nonsensical Avro decode.
        assertThatThrownBy(() -> reader.read(new byte[] {0x01, 0, 0, 0, 1, 0x2A}, quoteSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("magic byte");
    }

    @Test
    void aTruncatedPayloadIsRejected() {
        assertThatThrownBy(() -> reader.read(new byte[] {0x00, 0x00}, quoteSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shorter");

        assertThatThrownBy(() -> reader.read(null, quoteSchema))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void schemaIdOfReturnsMinusOneForUnframedInput() {
        assertThat(ConfluentAvroReader.schemaIdOf(new byte[] {0x01, 0, 0, 0, 1})).isEqualTo(-1);
        assertThat(ConfluentAvroReader.schemaIdOf(null)).isEqualTo(-1);
        assertThat(ConfluentAvroReader.schemaIdOf(new byte[] {0x00})).isEqualTo(-1);
    }

    /**
     * The consumer-side half of the schema evolution story. A producer that
     * adds a field must not break a service that has not been redeployed —
     * Avro resolution drops fields the reader does not know about.
     */
    @Test
    void aProducerCanAddAFieldWithoutBreakingThisService() throws IOException {
        String extended = """
            {"type":"record","name":"Quote","namespace":"io.tapeline.md","fields":[
              {"name":"venue","type":"string"},
              {"name":"symbol","type":"string"},
              {"name":"bid_price","type":"double"},
              {"name":"bid_size","type":"double"},
              {"name":"ask_price","type":"double"},
              {"name":"ask_size","type":"double"},
              {"name":"mid","type":"double"},
              {"name":"spread_bps","type":"double"},
              {"name":"imbalance","type":"double"},
              {"name":"event_time_us","type":"long"},
              {"name":"emit_time_us","type":"long"},
              {"name":"venue_latency_us","type":"long","default":0}
            ]}""";
        Schema writerSchema = new Schema.Parser().parse(extended);
        registry.preload(99, writerSchema);

        GenericData.Record record = new GenericData.Record(writerSchema);
        for (Schema.Field field : quoteSchema.getFields()) {
            record.put(field.name(), sampleQuote().get(field.name()));
        }
        record.put("venue_latency_us", 4242L);

        // Reader schema is still v1: the new field is simply skipped.
        QuoteSnapshot quote =
                QuoteSnapshot.fromAvro(reader.read(frame(99, record), quoteSchema));

        assertThat(quote.symbol()).isEqualTo("BTC-USD");
        assertThat(quote.bidPrice()).isEqualTo(64000.0);
    }

    @Test
    void aCrossedQuoteIsReportedInvalid() throws IOException {
        GenericData.Record crossed = (GenericData.Record) sampleQuote();
        crossed.put("bid_price", 65000.0);
        crossed.put("ask_price", 64000.0);

        QuoteSnapshot quote =
                QuoteSnapshot.fromAvro(reader.read(frame(SCHEMA_ID, crossed), quoteSchema));

        assertThat(quote.isValid()).isFalse();
    }
}
