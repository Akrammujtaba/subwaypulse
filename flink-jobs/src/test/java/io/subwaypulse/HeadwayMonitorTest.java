package io.subwaypulse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class HeadwayMonitorTest {
    // 8 AM in New York on a Monday
    static final long T0 = LocalDateTime.of(2026, 10, 26, 8, 0).atZone(ScheduleRow.NYC).toInstant().toEpochMilli();
    static final long DELAY = 60_000;

    static ScheduleRow sched(int headwayS) {
        ScheduleRow r = new ScheduleRow();
        r.routeId = "L";
        r.stopId = "L08N";
        r.localDate = "2026-10-26";
        r.hour = 8;
        r.scheduledHeadwaySeconds = headwayS;
        return r;
    }

    static Arrival arr(String trip, long offsetS) {
        long t = T0 + offsetS * 1000;
        return new Arrival(trip, "l", "L", "N", "L08N", t, t + 30_000, 0);
    }

    static KeyedTwoInputStreamOperatorTestHarness<String, Arrival, ScheduleRow, Headway> harness(int schedS)
            throws Exception {
        var op = new CoBroadcastWithKeyedOperator<>(new HeadwayMonitor(DELAY), List.of(HeadwayMonitor.SCHEDULE));
        var h = new KeyedTwoInputStreamOperatorTestHarness<>(op, Arrival::lineStopKey, r -> "unused", Types.STRING);
        h.open();
        h.processElement2(new StreamRecord<>(sched(schedS)));
        return h;
    }

    static List<Headway> headways(KeyedTwoInputStreamOperatorTestHarness<String, Arrival, ScheduleRow, Headway> h) {
        List<Headway> out = new ArrayList<>();
        for (Object o : h.getOutput()) {
            if (o instanceof StreamRecord<?> r) {
                out.add((Headway) r.getValue());
            }
        }
        return out;
    }

    static List<String> incidents(KeyedTwoInputStreamOperatorTestHarness<String, Arrival, ScheduleRow, Headway> h) {
        List<String> out = new ArrayList<>();
        var side = h.getSideOutput(HeadwayMonitor.INCIDENTS);
        if (side != null) {
            for (StreamRecord<Incident> r : side) {
                out.add(r.getValue().eventType);
            }
        }
        return out;
    }

    @Test
    void outOfOrderArrivalsAreSortedBeforeHeadwaysAreComputed() throws Exception {
        var h = harness(240);
        h.processElement1(new StreamRecord<>(arr("t2", 240)));
        h.processElement1(new StreamRecord<>(arr("t1", 0)));
        h.processElement1(new StreamRecord<>(arr("t3", 480)));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 480_000 + DELAY));
        var hw = headways(h);
        assertEquals(2, hw.size());
        assertEquals(240, hw.get(0).headwaySeconds);
        assertEquals("OK", hw.get(0).status);
        assertEquals(List.of(), incidents(h));
    }

    @Test
    void twoTrainsCloseTogetherAreBunching() throws Exception {
        var h = harness(240);
        h.processElement1(new StreamRecord<>(arr("t1", 0)));
        h.processElement1(new StreamRecord<>(arr("t2", 70)));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 70_000 + DELAY));
        assertEquals("BUNCHING", headways(h).get(0).status);
        assertEquals(List.of("BUNCHING"), incidents(h));
    }

    @Test
    void gapOpensWhileItIsHappeningAndClosesWhenTheNextTrainComes() throws Exception {
        var h = harness(240); // gap threshold 540s
        h.processElement1(new StreamRecord<>(arr("t1", 0)));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 539_000));
        assertEquals(List.of(), incidents(h));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 540_000));
        assertEquals(List.of("GAP_OPENED"), incidents(h));

        h.processElement1(new StreamRecord<>(arr("t2", 700)));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 700_000 + DELAY));
        assertEquals(List.of("GAP_OPENED", "GAP_CLOSED"), incidents(h));
        assertEquals("GAP", headways(h).get(0).status);
    }

    @Test
    void noGapWhenTheNextTrainCameBeforeTheDeadlineButIsNotReleasedYet() throws Exception {
        var h = harness(240); // gap threshold 540s
        h.processElement1(new StreamRecord<>(arr("t1", 0)));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + DELAY));
        // t2 arrived at 500s; it is held until 560s, after the gap deadline at 540s
        h.processElement1(new StreamRecord<>(arr("t2", 500)));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 540_000));
        assertEquals(List.of(), incidents(h));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 500_000 + DELAY));
        assertEquals(List.of(), incidents(h));
        assertEquals("OK", headways(h).get(0).status);
    }

    @Test
    void anArrivalOlderThanOneAlreadyReleasedIsOnlyCounted() throws Exception {
        var h = harness(240);
        h.processElement1(new StreamRecord<>(arr("t2", 240)));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 240_000 + DELAY));
        h.processElement1(new StreamRecord<>(arr("t1", 0)));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 300_000 + DELAY));
        assertTrue(headways(h).isEmpty());
    }

    @Test
    void noScheduleMeansNoGapAlarm() throws Exception {
        var op = new CoBroadcastWithKeyedOperator<>(new HeadwayMonitor(DELAY), List.of(HeadwayMonitor.SCHEDULE));
        var h = new KeyedTwoInputStreamOperatorTestHarness<String, Arrival, ScheduleRow, Headway>(
                op, Arrival::lineStopKey, r -> "unused", Types.STRING);
        h.open();
        h.processElement1(new StreamRecord<>(arr("t1", 0)));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 3_600_000));
        assertEquals(List.of(), incidents(h));
    }
}
