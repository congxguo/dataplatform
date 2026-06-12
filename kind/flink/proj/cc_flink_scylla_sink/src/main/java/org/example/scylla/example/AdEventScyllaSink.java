package org.example.scylla.example;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.example.AdEvent;
import org.example.scylla.ScyllaSink;
import org.example.scylla.ScyllaSinkConfig;

import java.time.Instant;
import java.util.UUID;

/**
 * Example ScyllaSink that writes ad-event records to the {@code ads.ad_events} table.
 *
 * <p>Depends on the {@code AdEvent} POJO from {@code cc_flink_event_window}. When using
 * this sink in a Flink job, add {@code cc-flink-scylla-sink} as a compile-scope dependency
 * in that project's pom.xml and include it in the shaded JAR.
 *
 * <p>Target table schema (from {@code scylla_schema.cql}):
 * <pre>
 * CREATE TABLE ads.ad_events (
 *     event_id      UUID PRIMARY KEY,
 *     event_type    TEXT,
 *     event_ts      TIMESTAMP,
 *     campaign_id   TEXT,
 *     user_id       TEXT,
 *     revenue       DOUBLE
 * );
 * </pre>
 *
 * <p>Usage:
 * <pre>{@code
 * ScyllaSinkConfig config = ScyllaSinkConfig.builder()
 *     .contactPoints("scylla-svc.scylla.svc.cluster.local", 9042)
 *     .localDatacenter("ldc1")
 *     .keyspace("ads")
 *     .batchSize(100)
 *     .flushIntervalMs(500)
 *     .build();
 *
 * stream.addSink(new AdEventScyllaSink(config));
 * }</pre>
 */
public class AdEventScyllaSink extends ScyllaSink<AdEvent> {

    public AdEventScyllaSink(ScyllaSinkConfig config) {
        super(config);
    }

    @Override
    protected String getInsertCql() {
        return "INSERT INTO ads.ad_events "
                + "(event_id, event_type, event_ts, campaign_id, user_id, revenue) "
                + "VALUES (:event_id, :event_type, :event_ts, :campaign_id, :user_id, :revenue)";
    }

    @Override
    protected BoundStatement bindRecord(PreparedStatement prepared, AdEvent event) {
        return prepared.bind()
                .setUuid("event_id",      UUID.fromString(event.event_id))
                .setString("event_type",  event.event_type)
                .setInstant("event_ts",   Instant.ofEpochMilli(event.event_time))
                .setString("campaign_id", event.campaign_id)
                .setString("user_id",     event.user_id)
                .setDouble("revenue",     event.revenue);
    }
}
