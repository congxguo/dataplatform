package org.example;

/**
 * Represents an ad event consumed from the Kafka topic "ads-events".
 *
 * JSON schema produced by the Python publisher:
 * {
 *   "event_id":    "uuid-string",
 *   "campaign_id": "campaign_001" | "campaign_002" | "campaign_003",
 *   "user_id":     "user_1" … "user_10",
 *   "event_type":  "impression" | "click" | "conversion",
 *   "revenue":     0.001 – 1.000,
 *   "event_time":  epoch-milliseconds (long)
 * }
 */
public class AdEvent {
    public String event_id;
    public String campaign_id;
    public String user_id;
    public String event_type;
    public double revenue;
    public long   event_time;   // epoch-ms — used as Flink event time

    public AdEvent() {}

    public AdEvent(
            String event_id,
            String campaign_id,
            String user_id,
            String event_type,
            double revenue,
            long   event_time) {

        this.event_id    = event_id;
        this.campaign_id = campaign_id;
        this.user_id     = user_id;
        this.event_type  = event_type;
        this.revenue     = revenue;
        this.event_time  = event_time;
    }

    @Override
    public String toString() {
        return "AdEvent{"
                + "event_id="   + event_id
                + ", campaign=" + campaign_id
                + ", user="     + user_id
                + ", type="     + event_type
                + ", revenue="  + revenue
                + ", event_time=" + event_time
                + "}";
    }
}
