package io.subwaypulse;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/** One scheduled headway, as published by the schedule loader to `schedule.headways`. */
public class ScheduleRow {
    public static final ZoneId NYC = ZoneId.of("America/New_York");

    public String routeId;
    public String stopId;
    public String localDate;
    public int hour;
    public int scheduledHeadwaySeconds;

    public ScheduleRow() {}

    public String key() {
        return key(routeId, stopId, localDate, hour);
    }

    public static String key(String routeId, String stopId, String localDate, int hour) {
        return routeId + "|" + stopId + "|" + localDate + "|" + hour;
    }

    /** Key for the scheduled headway in force at an instant (by NYC local clock). */
    public static String keyAt(String routeId, String stopId, long epochMs) {
        ZonedDateTime t = Instant.ofEpochMilli(epochMs).atZone(NYC);
        return key(routeId, stopId, t.toLocalDate().toString(), t.getHour());
    }
}
