package org.example;

public class Event {
    public String event_id;
    public String campaign_id;
    public String user_id;
    public String event_type;
    public double revenue;
    public long event_time;

    public Event() {}

    public Event(
            String event_id,
            String campaign_id,
            String user_id,
            String event_type,
            double revenue,
            long event_time) {

        this.event_id = event_id;
        this.campaign_id = campaign_id;
        this.user_id = user_id;
        this.event_type = event_type;
        this.revenue = revenue;
        this.event_time = event_time;
    }

    @Override
    public String toString() {
        return "Event{"
                + "user=" + user_id
                + ", campaign=" + campaign_id
                + ", type=" + event_type
                + ", revenue=" + revenue
                + ", event_time=" + event_time
                + "}";
    }
}