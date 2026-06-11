package org.example;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

public class EventTimeSlidingWindowJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // env.setParallelism(1);

        // ─────────────────────────────────────────────
        // 1. Source – generates random events every 10s
        //
        // Window size  = 60s, slide = 30s → each event
        // can appear in up to 2 overlapping windows.
        // ─────────────────────────────────────────────
        DataStream<Event> stream = env.addSource(new SourceFunction<Event>() {

            private volatile boolean running = true;

            private final String[] CAMPAIGNS   = {"c1", "c2", "c3"};
            private final String[] USERS       = {"u1", "u2", "u3", "u4"};
            private final String[] EVENT_TYPES = {"click", "view", "purchase"};

            @Override
            public void run(SourceContext<Event> ctx) throws Exception {
                Random rnd = new Random();
                long seq = 0;

                while (running) {
                    String id         = UUID.randomUUID().toString().substring(0, 8);
                    String campaign   = CAMPAIGNS[rnd.nextInt(CAMPAIGNS.length)];
                    String user       = USERS[rnd.nextInt(USERS.length)];
                    String type       = EVENT_TYPES[rnd.nextInt(EVENT_TYPES.length)];
                    double revenue    = Math.round(rnd.nextDouble() * 100.0 * 100.0) / 100.0;
                    long   eventTime  = System.currentTimeMillis();

                    Event e = new Event(id, campaign, user, type, revenue, eventTime);
                    ctx.collect(e);
                    System.out.println("[EMIT #" + (++seq) + "] " + e);

                    Thread.sleep(10_000);  // emit one event every 10s
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        // ─────────────────────────────────────────────
        // 2. Watermark Strategy
        // ─────────────────────────────────────────────
        WatermarkStrategy<Event> wm =
                WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(
                                Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                (event, ts) -> event.event_time)
                        .withIdleness(Duration.ofSeconds(10));

        DataStream<Event> withWm =
                stream.assignTimestampsAndWatermarks(wm);

        // ─────────────────────────────────────────────
        // 3. Debug: observe event flow
        // ─────────────────────────────────────────────
        withWm
                .map(e -> {
                    System.out.println("[RECV] " + e);
                    return e;
                });

        // ─────────────────────────────────────────────
        // 4. Sliding Event-Time Window
        //    size  = 60 seconds
        //    slide = 30 seconds
        //
        //  Windows opened:
        //    [0s,  60s),  [30s, 90s),  [60s, 120s), …
        //
        //  An event at t is included in every window
        //  whose [start, end) interval contains t,
        //  so events can appear in multiple windows.
        // ─────────────────────────────────────────────
        DataStream<String> result =
                withWm
                        .keyBy(e -> e.campaign_id)
                        .window(
                                SlidingEventTimeWindows.of(
                                        Time.seconds(60),  // window size
                                        Time.seconds(30)   // slide interval
                                )
                        )
                        .process(
                                new ProcessWindowFunction<
                                        Event,
                                        String,
                                        String,
                                        org.apache.flink.streaming.api.windowing.windows.TimeWindow>() {

                                    @Override
                                    public void process(
                                            String key,
                                            Context ctx,
                                            Iterable<Event> events,
                                            Collector<String> out) {

                                        long sum = 0;
                                        int count = 0;

                                        System.out.println(
                                                "\n[WINDOW FIRE] "
                                                        + key
                                                        + " start="
                                                        + Instant.ofEpochMilli(
                                                                ctx.window().getStart())
                                                        + " end="
                                                        + Instant.ofEpochMilli(
                                                                ctx.window().getEnd())
                                        );

                                        for (Event e : events) {
                                            sum += e.revenue;
                                            count++;
                                            System.out.println(
                                                    "  included=" + e
                                                    + " event_time=" + Instant.ofEpochMilli(e.event_time)
                                                    );
                                        }

                                        out.collect(
                                                "campaign="
                                                        + key
                                                        + ", window=["
                                                        + Instant.ofEpochMilli(ctx.window().getStart())
                                                        + ", "
                                                        + Instant.ofEpochMilli(ctx.window().getEnd())
                                                        + "), count="
                                                        + count
                                                        + ", sum="
                                                        + sum
                                        );
                                    }
                                }
                        );

        // ─────────────────────────────────────────────
        // 5. Output
        // ─────────────────────────────────────────────
        result.print();

        env.execute("Event Time + Watermark + Sliding Window Demo");
    }

}
