package io.subwaypulse;

/** What HeadwayMonitor receives as broadcast state: a schedule row or a feed outage. */
public class Reference {
    public ScheduleRow schedule;
    public Outage outage;

    public Reference() {}

    public static Reference of(ScheduleRow row) {
        Reference r = new Reference();
        r.schedule = row;
        return r;
    }

    public static Reference of(Outage outage) {
        Reference r = new Reference();
        r.outage = outage;
        return r;
    }
}
