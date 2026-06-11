package org.example;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

public class EventTimeSlidingWindowJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // env.setParallelism(1);

        // ─────────────────────────────────────────────
        // 1. Source (replace with Kafka in your setup)
        //
        // Window size  = 60s, slide = 30s → each event
        // can appear in up to 2 overlapping windows.
        // ─────────────────────────────────────────────
        DataStream<Event> stream = env
                .fromElements(
                        // window [0s, 60s)
                        new Event("1", "c1", "u1", "click", 1.0, 10000L),
                        new Event("2", "c1", "u1", "click", 2.0, 20000L),

                        // out-of-order event (arrives late, still within watermark tolerance)
                        new Event("3", "c1", "u1", "click", 3.0, 15000L),

                        // falls in [0s,60s) and [30s,90s)
                        new Event("4", "c1", "u1", "click", 4.0, 50000L),

                        // falls in [30s,90s) only
                        new Event("5", "c1", "u1", "click", 5.0, 70000L),

                        // advances watermark far enough to close earlier windows
                        new Event("6", "c1", "u1", "click", 6.0, 130000L)
                );

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
                                                    + " wall=" + Instant.ofEpochMilli(e.event_time));
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
