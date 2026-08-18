package io.tapeline.serving.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.springframework.stereotype.Component;

/**
 * Decodes the Confluent Avro wire format the ingestion and stream tiers write.
 *
 * <pre>
 *   byte 0      magic byte, always 0x00
 *   bytes 1..4  schema id, big-endian int32
 *   bytes 5..   Avro binary payload
 * </pre>
 *
 * <p>Deliberately hand-written rather than pulled from the vendor serializer
 * library, for the same reason the Go tier owns its registry client: this is
 * the interoperability contract between three languages in this repository,
 * and it is forty lines. Owning it also means the reader schema is explicit
 * at the call site, so schema resolution is visible rather than implied.
 */
@Component
public class ConfluentAvroReader {

    public static final byte MAGIC_BYTE = 0x00;
    public static final int HEADER_LENGTH = 5;

    private final SchemaRegistryClient registry;

    public ConfluentAvroReader(SchemaRegistryClient registry) {
        this.registry = registry;
    }

    /**
     * Decodes a framed record against an explicit reader schema.
     *
     * <p>The writer schema comes from the registry by id; the reader schema is
     * this service's compiled-in expectation. Avro resolves the two, which is
     * what allows the producer to add fields without redeploying this service.
     */
    public GenericRecord read(byte[] framed, Schema readerSchema) {
        if (framed == null || framed.length < HEADER_LENGTH) {
            throw new IllegalArgumentException(
                    "payload is shorter than the Confluent wire header");
        }
        if (framed[0] != MAGIC_BYTE) {
            throw new IllegalArgumentException(String.format(
                    "not Confluent-framed: magic byte was 0x%02x, expected 0x00", framed[0]));
        }

        int schemaId = ByteBuffer.wrap(framed, 1, 4).getInt();
        Schema writerSchema = registry.byId(schemaId);

        GenericDatumReader<GenericRecord> reader =
                new GenericDatumReader<>(writerSchema, readerSchema);
        BinaryDecoder decoder = DecoderFactory.get()
                .binaryDecoder(framed, HEADER_LENGTH, framed.length - HEADER_LENGTH, null);

        try {
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "failed to decode Avro payload with schema id " + schemaId, e);
        }
    }

    /** Extracts the schema id without decoding. Useful in logs when a decode
     * fails and the interesting question is which schema was involved. */
    public static int schemaIdOf(byte[] framed) {
        if (framed == null || framed.length < HEADER_LENGTH || framed[0] != MAGIC_BYTE) {
            return -1;
        }
        return ByteBuffer.wrap(framed, 1, 4).getInt();
    }
}
