package io.subwaypulse;

/**
 * What HeadwayMonitor receives as broadcast state: a schedule row, a feed outage, or
 * word that a feed was observed up to a given time.
 */
public class Reference {
    public ScheduleRow schedule;
    public Outage outage;
    /** Set together: feed {@code seenFeedId} was fetched successfully at {@code seenTs}. */
    public String seenFeedId;
    public long seenTs;

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

    public static Reference seen(String feedId, long ts) {
        Reference r = new Reference();
        r.seenFeedId = feedId;
        r.seenTs = ts;
        return r;
    }
}
