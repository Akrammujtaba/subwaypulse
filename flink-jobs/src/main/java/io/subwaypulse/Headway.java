package io.subwaypulse;

/** Time between two consecutive trains of one route at one stop. */
public class Headway {
    public String routeId;
    public String direction;
    public String stopId;
    public String tripKey;
    public String prevTripKey;
    public long arrivalTs;
    public long prevArrivalTs;
    public long headwaySeconds;
    /** Null when the schedule has no service for that hour. */
    public Integer scheduledHeadwaySeconds;
    /** OK, BUNCHING, GAP or UNSCHEDULED. */
    public String status;
    public long detectedAt;

    public Headway() {}
}
