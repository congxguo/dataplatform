package org.example;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.scylla.ScyllaSinkConfig;
import org.example.scylla.example.AdEventScyllaSink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

/**
 * Extends the sliding-window job with a ScyllaDB sink.
 *
 * Pipeline:
 *
 *   Kafka (ads-events)
 *       │
 *       ▼
 *   Watermark assignment (bounded out-of-orderness 5 s)
 *       │
 *       ├──► AdEventScyllaSink ──► ads.ad_events (ScyllaDB)
 *       │      raw events, batched 100 / 500 ms
 *       │
 *       └──► keyBy(campaign_id)
 *                │
 *                ▼
 *            SlidingEventTimeWindow (size=60 s, slide=30 s)
 *                │
 *                ▼
 *            per-campaign aggregation (count, revenue, type breakdown)
 *                │
 *                ▼
 *            print() — TaskManager stdout
 */
public class KafkaScyllaSinkJob {

    private static final String KAFKA_BOOTSTRAP =
            "kafka-main-kafka-bootstrap.kafka.svc.cluster.local:9092";
    private static final String TOPIC    = "ads-events";
    private static final String GROUP_ID = "flink-kafka-scylla-sink";

    // ScyllaDB operator creates a client service named <cluster-name>-client
    private static final String SCYLLA_HOST = "scylla-local-client.scylla.svc.cluster.local";
    private static final String SCYLLA_DC   = "ldc1";
    private static final String SCYLLA_KS   = "ads";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // ─────────────────────────────────────────────
        // 1. Kafka Source
        //
        // Reads from the latest offset so the job only
        // processes events produced after it starts.
        // JSON bytes are deserialized into AdEvent POJOs.
        // ─────────────────────────────────────────────
        KafkaSource<AdEvent> source = KafkaSource.<AdEvent>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(TOPIC)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new AbstractDeserializationSchema<AdEvent>() {
                    private final ObjectMapper mapper = new ObjectMapper();

                    @Override
                    public AdEvent deserialize(byte[] message) throws IOException {
                        JsonNode node = mapper.readTree(message);
                        return new AdEvent(
                                node.get("event_id").asText(),
                                node.get("campaign_id").asText(),
                                node.get("user_id").asText(),
                                node.get("event_type").asText(),
                                node.get("revenue").asDouble(),
                                node.get("event_time").asLong()
                        );
                    }
                })
                .build();

        DataStream<AdEvent> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka ads-events"
        );

        // ─────────────────────────────────────────────
        // 2. Watermark Strategy
        //
        // BoundedOutOfOrderness(5 s) handles minor
        // Kafka delivery reordering.  idleness(30 s)
        // unsticks the watermark when a partition goes
        // quiet (prevents windows from never firing).
        // ─────────────────────────────────────────────
        WatermarkStrategy<AdEvent> wm =
                WatermarkStrategy
                        .<AdEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, ts) -> event.event_time)
                        .withIdleness(Duration.ofSeconds(30));

        DataStream<AdEvent> withWm = stream.assignTimestampsAndWatermarks(wm);

        // ─────────────────────────────────────────────
        // 3. ScyllaDB Sink — persist raw events
        //
        // Raw AdEvents are written to ads.ad_events
        // before windowing, so every event is stored
        // regardless of whether it falls in a window.
        //
        // Batched: flush every 100 records or 500 ms,
        // whichever comes first (at-least-once).
        // ─────────────────────────────────────────────
        ScyllaSinkConfig scyllaConfig = ScyllaSinkConfig.builder()
                .contactPoints(SCYLLA_HOST, 9042)
                .localDatacenter(SCYLLA_DC)
                .keyspace(SCYLLA_KS)
                .username("cassandra")
                .password("cassandra")
                .batchSize(100)
                .flushIntervalMs(500)
                .build();

        withWm.addSink(new AdEventScyllaSink(scyllaConfig));

        // ─────────────────────────────────────────────
        // 4. Sliding Event-Time Window
        //    size  = 60 s
        //    slide = 30 s
        //
        //  Keyed by campaign_id so each campaign gets
        //  its own independent set of sliding windows.
        // ─────────────────────────────────────────────
        DataStream<String> result =
                withWm
                        .keyBy(e -> e.campaign_id)
                        .window(SlidingEventTimeWindows.of(
                                Time.seconds(60),   // window size
                                Time.seconds(30)    // slide interval
                        ))
                        .process(new ProcessWindowFunction<AdEvent, String, String, TimeWindow>() {

                            @Override
                            public void process(
                                    String key,
                                    Context ctx,
                                    Iterable<AdEvent> events,
                                    Collector<String> out) {

                                int    count       = 0;
                                int    impressions = 0;
                                int    clicks      = 0;
                                int    conversions = 0;
                                double totalRevenue = 0.0;

                                String windowStart = Instant.ofEpochMilli(ctx.window().getStart()).toString();
                                String windowEnd   = Instant.ofEpochMilli(ctx.window().getEnd()).toString();

                                System.out.println(
                                        "\n[WINDOW FIRE] campaign=" + key
                                        + " window=[" + windowStart + ", " + windowEnd + ")"
                                );

                                for (AdEvent e : events) {
                                    count++;
                                    totalRevenue += e.revenue;
                                    switch (e.event_type) {
                                        case "impression":  impressions++;  break;
                                        case "click":       clicks++;       break;
                                        case "conversion":  conversions++;  break;
                                    }
                                    System.out.println("  " + e);
                                }

                                String summary = String.format(
                                        "campaign=%s, window=[%s, %s), count=%d, "
                                        + "impressions=%d, clicks=%d, conversions=%d, "
                                        + "totalRevenue=%.3f",
                                        key, windowStart, windowEnd, count,
                                        impressions, clicks, conversions,
                                        totalRevenue
                                );

                                System.out.println("[RESULT] " + summary);
                                out.collect(summary);
                            }
                        });

        // ─────────────────────────────────────────────
        // 5. Output — printed to TaskManager stdout/logs
        // ─────────────────────────────────────────────
        result.print();

        env.execute("Kafka Scylla Sink Job");
    }
}
