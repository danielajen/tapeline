package io.tapeline.serving.quotes;

import org.apache.avro.generic.GenericRecord;

/**
 * The serving tier's view of a quote.
 *
 * <p>A separate type from the generated protobuf {@code Quote} on purpose.
 * The protobuf message is a wire contract with clients and changing it is a
 * compatibility event; this record is internal and free to change. Collapsing
 * them would make every internal refactor a public API change.
 */
public record QuoteSnapshot(
        String venue,
        String symbol,
        double bidPrice,
        double bidSize,
        double askPrice,
        double askSize,
        double mid,
        double spreadBps,
        double imbalance,
        long eventTimeUs,
        long emitTimeUs) {

    public static QuoteSnapshot fromAvro(GenericRecord r) {
        return new QuoteSnapshot(
                str(r, "venue"),
                str(r, "symbol"),
                dbl(r, "bid_price"),
                dbl(r, "bid_size"),
                dbl(r, "ask_price"),
                dbl(r, "ask_size"),
                dbl(r, "mid"),
                dbl(r, "spread_bps"),
                dbl(r, "imbalance"),
                lng(r, "event_time_us"),
                lng(r, "emit_time_us"));
    }

    /** Age against a supplied wall clock, in microseconds. Callers pass the
     * clock rather than reading it here so staleness is testable. */
    public long ageUs(long nowUs) {
        return nowUs - eventTimeUs;
    }

    public boolean isValid() {
        return bidPrice > 0 && askPrice > 0 && askPrice >= bidPrice;
    }

    // Avro hands back Utf8 for strings, so toString is required rather than a
    // cast. See the note in the stream tier's Codec for the same trap.
    private static String str(GenericRecord r, String field) {
        Object v = r.get(field);
        return v == null ? "" : v.toString();
    }

    private static double dbl(GenericRecord r, String field) {
        Object v = r.get(field);
        return v instanceof Number n ? n.doubleValue() : 0.0;
    }

    private static long lng(GenericRecord r, String field) {
        Object v = r.get(field);
        return v instanceof Number n ? n.longValue() : 0L;
    }
}
