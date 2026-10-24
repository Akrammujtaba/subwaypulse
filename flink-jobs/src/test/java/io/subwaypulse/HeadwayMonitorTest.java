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
        return sched(8, headwayS);
    }

    static ScheduleRow sched(int hour, int headwayS) {
        ScheduleRow r = new ScheduleRow();
        r.routeId = "L";
        r.stopId = "L08N";
        r.localDate = "2026-10-26";
        r.hour = hour;
        r.scheduledHeadwaySeconds = headwayS;
        return r;
    }

    static Arrival arr(String trip, long offsetS) {
        long t = T0 + offsetS * 1000;
        return new Arrival(trip, "l", "L", "N", "L08N", t, t + 30_000, 0);
    }

    static KeyedTwoInputStreamOperatorTestHarness<String, Arrival, Reference, Headway> harness(int schedS)
            throws Exception {
        var op = new CoBroadcastWithKeyedOperator<>(new HeadwayMonitor(DELAY), List.of(HeadwayMonitor.SCHEDULE, HeadwayMonitor.OUTAGES, HeadwayMonitor.LAST_SEEN));
        var h = new KeyedTwoInputStreamOperatorTestHarness<>(op, Arrival::lineStopKey, r -> "unused", Types.STRING);
        h.open();
        h.processElement2(new StreamRecord<>(Reference.of(sched(schedS))));
        return h;
    }

    static List<Headway> headways(KeyedTwoInputStreamOperatorTestHarness<String, Arrival, Reference, Headway> h) {
        List<Headway> out = new ArrayList<>();
        for (Object o : h.getOutput()) {
            if (o instanceof StreamRecord<?> r) {
                out.add((Headway) r.getValue());
            }
        }
        return out;
    }

    static List<String> incidents(KeyedTwoInputStreamOperatorTestHarness<String, Arrival, Reference, Headway> h) {
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
    void acrossAnHourBoundaryAGapIsJudgedOnTheScheduleWhenThePreviousTrainCame() throws Exception {
        var h = harness(300);                                    // 8 AM: every 5 min, gap at 600s
        h.processElement2(new StreamRecord<>(Reference.of(sched(7, 420))));    // 7 AM: every 7 min, gap at 840s
        h.processElement1(new StreamRecord<>(arr("t1", -240)));  // 7:56
        h.processElement1(new StreamRecord<>(arr("t2", 420)));   // 8:07, 660s later
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 420_000 + DELAY));
        assertEquals("OK", headways(h).get(0).status);
        assertEquals(List.of(), incidents(h));
    }

    static Reference outage(long fromS, long toS) {
        return Reference.of(new Outage("l", T0 + fromS * 1000, T0 + toS * 1000));
    }

    @Test
    void noGapAlarmWhenTheFeedWasDownDuringTheWait() throws Exception {
        var h = harness(240); // gap threshold 540s
        h.processElement1(new StreamRecord<>(arr("t1", 0)));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + DELAY));
        // No fetch succeeded from 200s to 900s (say the machine slept), then data resumes
        h.processElement2(new StreamRecord<>(outage(200, 900)));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 900_000));
        assertEquals(List.of(), incidents(h));
    }

    @Test
    void noHeadwayAcrossAnOutageAndTheNextOneIsMeasuredNormally() throws Exception {
        var h = harness(240);
        h.processElement2(new StreamRecord<>(outage(200, 900)));
        h.processElement1(new StreamRecord<>(arr("t1", 0)));
        h.processElement1(new StreamRecord<>(arr("t2", 950)));   // a train in between may have been missed
        h.processElement1(new StreamRecord<>(arr("t3", 1190)));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 1190_000 + DELAY));
        var hw = headways(h);
        assertEquals(1, hw.size());
        assertEquals("t3", hw.get(0).tripKey);
        assertEquals(240, hw.get(0).headwaySeconds);
        assertEquals(List.of(), incidents(h));
    }

    @Test
    void anOutageOnAnotherFeedChangesNothing() throws Exception {
        var h = harness(240);
        h.processElement1(new StreamRecord<>(arr("t1", 0)));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + DELAY));
        h.processElement2(new StreamRecord<>(Reference.of(new Outage("ace", T0 + 200_000, T0 + 900_000))));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 540_000));
        assertEquals(List.of("GAP_OPENED"), incidents(h));
    }

    static Reference seen(long atS) {
        return Reference.seen("l", T0 + atS * 1000);
    }

    @Test
    void noGapAlarmWhileOnlyOtherFeedsMoveEventTimeAndThisOneIsDown() throws Exception {
        var h = harness(240); // gap threshold 540s
        h.processElement1(new StreamRecord<>(arr("t1", 0)));
        h.processElement2(new StreamRecord<>(seen(200)));        // last fetch of this feed before it went down
        // Other feeds carry event time past the deadline; this one's outage isn't known yet
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 700_000));
        assertEquals(List.of(), incidents(h));
        // The feed comes back at 900s: the outage, then the heartbeat that ended it
        h.processElement2(new StreamRecord<>(outage(200, 900)));
        h.processElement2(new StreamRecord<>(seen(900)));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 960_000));
        assertEquals(List.of(), incidents(h));
    }

    @Test
    void noGapAlarmWhenTheOutageNoticeIsStillOnItsWayAfterAllFeedsComeBack() throws Exception {
        var h = harness(240); // gap threshold 540s
        h.processElement1(new StreamRecord<>(arr("t1", 0)));
        h.processElement2(new StreamRecord<>(seen(200)));
        // Every feed is down, so the reference input goes idle. When the feeds return,
        // the arrivals input moves event time first, before the outage notice arrives.
        h.processWatermarkStatus2(org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus.IDLE);
        h.processWatermark1(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 900_000));
        assertEquals(List.of(), incidents(h));
        h.processWatermarkStatus2(org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus.ACTIVE);
        h.processElement2(new StreamRecord<>(outage(200, 900)));
        h.processElement2(new StreamRecord<>(seen(900)));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 960_000));
        assertEquals(List.of(), incidents(h));
    }

    @Test
    void aGapWaitsForTheFeedAndThenOpensAtTheDeadline() throws Exception {
        var h = harness(240); // gap threshold 540s
        h.processElement1(new StreamRecord<>(arr("t1", 0)));
        h.processElement2(new StreamRecord<>(seen(530)));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 545_000));
        assertEquals(List.of(), incidents(h));                   // feed not yet seen past 540s
        h.processElement2(new StreamRecord<>(seen(545)));        // seen, no outage: the gap is real
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 560_000));
        assertEquals(List.of("GAP_OPENED"), incidents(h));
        assertEquals(T0 + 540_000, h.getSideOutput(HeadwayMonitor.INCIDENTS).peek().getValue().eventTs);
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
        var op = new CoBroadcastWithKeyedOperator<>(new HeadwayMonitor(DELAY), List.of(HeadwayMonitor.SCHEDULE, HeadwayMonitor.OUTAGES, HeadwayMonitor.LAST_SEEN));
        var h = new KeyedTwoInputStreamOperatorTestHarness<String, Arrival, Reference, Headway>(
                op, Arrival::lineStopKey, r -> "unused", Types.STRING);
        h.open();
        h.processElement1(new StreamRecord<>(arr("t1", 0)));
        h.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(T0 + 3_600_000));
        assertEquals(List.of(), incidents(h));
    }
}
