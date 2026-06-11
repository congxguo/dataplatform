package org.example;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

public class EventTimeTumblingWindowJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // ─────────────────────────────────────────────
        // 1. Source – generates random events every 10s
        //
        // Runs indefinitely (until cancelled) so the job
        // stays RUNNING on k8s and you can inspect logs.
        // Each event uses wall-clock time as event_time,
        // so the 30s tumbling window fires roughly every
        // 3 events.
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
                    System.out.println(
                            "[RECV] " + e);
                    return e;
                });

        // ─────────────────────────────────────────────
        // 4. Tumbling Event-Time Window
        // ─────────────────────────────────────────────
        DataStream<String> result =
                withWm
                        .keyBy(e -> e.campaign_id)
                        .window(
                                TumblingEventTimeWindows.of(
                                        Time.seconds(30)
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
                                                    + " event_time=" + Instant.ofEpochMilli(e.event_time));
                                        }

                                        out.collect(
                                                "campaign="
                                                        + key
                                                        + ", count="
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

        env.execute("Event Time + Watermark + Tumbling Window Demo");
    }

}
