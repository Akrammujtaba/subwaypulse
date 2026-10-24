package io.subwaypulse;

/**
 * A stretch of event time in which one feed was not observed: no successful fetch
 * between {@code fromTs} and {@code toTs} (epoch ms, feed time). Nothing can be
 * concluded about trains on that feed's lines in between, in particular not that
 * none came.
 */
public class Outage {
    public String feedId;
    public long fromTs;
    public long toTs;

    public Outage() {}

    public Outage(String feedId, long fromTs, long toTs) {
        this.feedId = feedId;
        this.fromTs = fromTs;
        this.toTs = toTs;
    }

    public String key() {
        return feedId + "|" + fromTs;
    }

    @Override
    public String toString() {
        return "Outage{" + feedId + " " + fromTs + ".." + toTs + "}";
    }
}
