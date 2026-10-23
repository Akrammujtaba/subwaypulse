package io.subwaypulse;

/**
 * A service problem at one stop. Written as an append-only event log:
 * BUNCHING (one event), or GAP_OPENED followed later by GAP_CLOSED.
 */
public class Incident {
    public String incidentId;
    public String eventType;
    public String routeId;
    public String direction;
    public String stopId;
    /** Arrival of the train before the problem (gap start, or the first of two bunched trains). */
    public long prevArrivalTs;
    public String prevTripKey;
    /** Arrival that ended the gap or completed the bunch. Null while a gap is still open. */
    public Long arrivalTs;
    public String tripKey;
    /** Event time at which the problem became knowable (epoch ms). */
    public long eventTs;
    public Long headwaySeconds;
    public int scheduledHeadwaySeconds;
    public int thresholdSeconds;
    public long detectedAt;

    public Incident() {}

    public String toJson() {
        StringBuilder sb = new StringBuilder("{");
        field(sb, "incident_id", incidentId).append(',');
        field(sb, "event_type", eventType).append(',');
        field(sb, "route_id", routeId).append(',');
        field(sb, "direction", direction).append(',');
        field(sb, "stop_id", stopId).append(',');
        sb.append("\"prev_arrival_ts\":").append(prevArrivalTs).append(',');
        field(sb, "prev_trip_key", prevTripKey).append(',');
        sb.append("\"arrival_ts\":").append(arrivalTs).append(',');
        field(sb, "trip_key", tripKey).append(',');
        sb.append("\"event_ts\":").append(eventTs).append(',');
        sb.append("\"headway_s\":").append(headwaySeconds).append(',');
        sb.append("\"scheduled_headway_s\":").append(scheduledHeadwaySeconds).append(',');
        sb.append("\"threshold_s\":").append(thresholdSeconds).append(',');
        sb.append("\"detected_at\":").append(detectedAt);
        return sb.append('}').toString();
    }

    private static StringBuilder field(StringBuilder sb, String k, String v) {
        sb.append('"').append(k).append("\":");
        if (v == null) {
            return sb.append("null");
        }
        return sb.append('"').append(v.replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
    }
}
