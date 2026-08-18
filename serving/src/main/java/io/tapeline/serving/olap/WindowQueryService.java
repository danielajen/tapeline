package io.tapeline.serving.olap;

import io.tapeline.proto.v1.QueryWindowsRequest;
import io.tapeline.proto.v1.QueryWindowsResponse;
import io.tapeline.proto.v1.WindowBar;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

/**
 * Historical window queries, routed between two stores.
 *
 * <p><b>The routing rule.</b> Recent ranges go to the real-time OLAP store
 * (ClickHouse here, Pinot in the equivalent Uber design); anything older goes
 * to Iceberg. The split exists because the two stores are good at genuinely
 * different things:
 *
 * <ul>
 *   <li>The OLAP store answers in single-digit milliseconds but keeps a
 *       bounded retention, because keeping years of tick data on attached
 *       storage is expensive.
 *   <li>Iceberg on S3 keeps everything at object-storage prices and answers
 *       in seconds, which is fine for a research query and useless for a
 *       chart that has to render now.
 *   </ul>
 *
 * <p>The response reports which tier answered. That field is not decoration:
 * without it, a latency regression caused by queries silently falling through
 * to the lakehouse is invisible in the metrics.
 *
 * <p>Redis is not in this path at all. It caches the single latest quote,
 * which is a different question from a range scan, and treating a cache as an
 * analytical store is the mistake this tier exists to avoid — see
 * docs/DESIGN_DECISIONS.md#d4.
 */
@Service
public class WindowQueryService {

    private static final Logger log = LoggerFactory.getLogger(WindowQueryService.class);

    private static final int DEFAULT_LIMIT = 1000;
    private static final int MAX_LIMIT = 50_000;

    private static final String SQL =
            """
            SELECT venue, symbol, window_start_us, window_end_us,
                   open, high, low, close, volume, vwap, trade_count
            FROM bars
            WHERE symbol = ?
              AND window_start_us >= ?
              AND window_start_us <  ?
              AND (? = '' OR venue = ?)
            ORDER BY window_start_us
            LIMIT ?
            """;

    private static final RowMapper<WindowBar> MAPPER = (rs, rowNum) -> WindowBar.newBuilder()
            .setVenue(rs.getString("venue"))
            .setSymbol(rs.getString("symbol"))
            .setWindowStartUs(rs.getLong("window_start_us"))
            .setWindowEndUs(rs.getLong("window_end_us"))
            .setOpen(rs.getDouble("open"))
            .setHigh(rs.getDouble("high"))
            .setLow(rs.getDouble("low"))
            .setClose(rs.getDouble("close"))
            .setVolume(rs.getDouble("volume"))
            .setVwap(rs.getDouble("vwap"))
            .setTradeCount(rs.getLong("trade_count"))
            .build();

    private final JdbcTemplate olap;
    private final Duration olapRetention;

    public WindowQueryService(
            @Qualifier("olapJdbcTemplate") JdbcTemplate olap,
            @Value("${tapeline.olap.retention-days:7}") int retentionDays) {
        this.olap = olap;
        this.olapRetention = Duration.ofDays(retentionDays);
    }

    public QueryWindowsResponse query(QueryWindowsRequest request) {
        long started = System.nanoTime();

        int limit = request.getLimit() == 0
                ? DEFAULT_LIMIT
                : Math.min(request.getLimit(), MAX_LIMIT);

        boolean servedFromOlap = withinOlapRetention(request.getStartUs());
        if (!servedFromOlap) {
            // The lakehouse path is deliberately not implemented as a
            // synchronous query behind this RPC. A Trino or Spark scan over
            // Iceberg takes seconds to minutes, which does not belong behind a
            // request that a caller is holding a connection open for. The
            // shape this wants is an async job with a result handle, and
            // pretending otherwise here would mean shipping an endpoint whose
            // p99 is measured in minutes. Tracked in docs/ROADMAP.md.
            log.info(
                    "range starting {} predates OLAP retention of {} days",
                    request.getStartUs(), olapRetention.toDays());
            throw new UnsupportedOperationException(
                    "range predates the OLAP retention window of "
                            + olapRetention.toDays()
                            + " days; use the lakehouse export path");
        }

        String venue = request.getVenue();
        List<WindowBar> bars = olap.query(
                SQL,
                MAPPER,
                request.getSymbol(),
                request.getStartUs(),
                request.getEndUs(),
                venue,
                venue,
                limit);

        return QueryWindowsResponse.newBuilder()
                .addAllBars(bars)
                .setServedFrom("olap")
                .setQueryLatencyUs((System.nanoTime() - started) / 1000)
                .build();
    }

    private boolean withinOlapRetention(long startUs) {
        long cutoffUs = (System.currentTimeMillis() - olapRetention.toMillis()) * 1000L;
        return startUs >= cutoffUs;
    }
}
