package io.subwaypulse;

/** A train reached a stop. Inferred from the feed, not reported directly by it. */
public class Arrival {
    public String tripKey;
    public String feedId;
    public String routeId;
    public String direction;
    public String stopId;
    /** When the train arrived (epoch ms). For a first stop, when it left. */
    public long arrivalTs;
    /** Feed timestamp of the update that showed the train had passed (epoch ms). */
    public long observedAt;
    /** Wall-clock time the job produced this record (epoch ms). */
    public long detectedAt;

    public Arrival() {}

    public Arrival(String tripKey, String feedId, String routeId, String direction, String stopId,
                   long arrivalTs, long observedAt, long detectedAt) {
        this.tripKey = tripKey;
        this.feedId = feedId;
        this.routeId = routeId;
        this.direction = direction;
        this.stopId = stopId;
        this.arrivalTs = arrivalTs;
        this.observedAt = observedAt;
        this.detectedAt = detectedAt;
    }

    /** Headways are measured per route at a directional stop (stop ids end in N or S). */
    public String lineStopKey() {
        return routeId + "|" + stopId;
    }

    @Override
    public String toString() {
        return "Arrival{" + tripKey + " " + routeId + " " + stopId + " @" + arrivalTs + "}";
    }
}
