package io.subwaypulse;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Keyed by route and stop. Turns arrivals into headways and raises incidents.
 *
 * <p>Arrivals at one stop come from different trains, which sit on different Kafka
 * partitions and reach this operator in no fixed order. Each arrival is held until
 * the watermark passes its arrival time plus {@code releaseDelayMs}, then released in
 * arrival-time order. A longer delay tolerates more disorder but detects later.
 *
 * <p>Gaps are detected while they are happening: after each train, an event-time
 * timer is set for when the next one would be overdue. If it fires before another
 * train arrives, a GAP_OPENED incident goes out right away. The schedule (per route,
 * stop, date, and hour) arrives as broadcast state, so it can be refreshed without
 * restarting the job.
 */
public class HeadwayMonitor extends KeyedBroadcastProcessFunction<String, Arrival, ScheduleRow, Headway> {
    public static final MapStateDescriptor<String, Integer> SCHEDULE =
            new MapStateDescriptor<>("schedule", Types.STRING, Types.INT);
    public static final OutputTag<Incident> INCIDENTS = new OutputTag<>("incidents") {};
    /** Keep schedule rows this many days before the newest date seen. */
    static final int SCHEDULE_KEEP_DAYS = 2;
    static final long SCHEDULE_WAIT_MS = 15_000;

    private final long releaseDelayMs;
    /** Newest schedule date this instance has seen; only used to decide when to prune. */
    private transient String newestDate;

    private transient ValueState<ArrayList<Arrival>> pending;
    private transient ValueState<Arrival> lastReleased;
    private transient ValueState<Long> gapTimer;
    private transient ValueState<Integer> gapThreshold;
    private transient ValueState<String> openGapId;
    private transient Counter lateArrivals;
    private transient Counter unscheduled;
    private transient Counter bunching;
    private transient Counter gaps;

    public HeadwayMonitor(long releaseDelayMs) {
        this.releaseDelayMs = releaseDelayMs;
    }

    @Override
    public void open(Configuration parameters) {
        pending = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pending", TypeInformation.of(new TypeHint<ArrayList<Arrival>>() {})));
        lastReleased = getRuntimeContext().getState(new ValueStateDescriptor<>("last", Arrival.class));
        gapTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("gapTimer", Types.LONG));
        gapThreshold = getRuntimeContext().getState(new ValueStateDescriptor<>("gapThreshold", Types.INT));
        openGapId = getRuntimeContext().getState(new ValueStateDescriptor<>("openGapId", Types.STRING));
        lateArrivals = getRuntimeContext().getMetricGroup().counter("lateArrivals");
        unscheduled = getRuntimeContext().getMetricGroup().counter("unscheduledHeadways");
        bunching = getRuntimeContext().getMetricGroup().counter("bunchingIncidents");
        gaps = getRuntimeContext().getMetricGroup().counter("gapIncidents");
    }

    @Override
    public void processElement(Arrival a, ReadOnlyContext ctx, Collector<Headway> out) throws Exception {
        ArrayList<Arrival> buf = pending.value();
        if (buf == null) {
            buf = new ArrayList<>();
        }
        buf.add(a);
        pending.update(buf);
        ctx.timerService().registerEventTimeTimer(a.arrivalTs + releaseDelayMs);
    }

    @Override
    public void processBroadcastElement(ScheduleRow row, Context ctx, Collector<Headway> out) throws Exception {
        BroadcastState<String, Integer> state = ctx.getBroadcastState(SCHEDULE);
        state.put(row.key(), row.scheduledHeadwaySeconds);
        if (newestDate != null && row.localDate.compareTo(newestDate) <= 0) {
            return;
        }
        // First row of a newer date: drop dates well before it so the state doesn't
        // grow forever. Only on a new date, because a full scan per row made loading
        // ~70k rows quadratic.
        newestDate = row.localDate;
        String cutoff = LocalDate.parse(row.localDate).minusDays(SCHEDULE_KEEP_DAYS).toString();
        Iterator<Map.Entry<String, Integer>> it = state.iterator();
        while (it.hasNext()) {
            String[] parts = it.next().getKey().split("\\|");
            if (parts[2].compareTo(cutoff) < 0) {
                it.remove();
            }
        }
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Headway> out) throws Exception {
        ArrayList<Arrival> buf = pending.value();
        if (buf != null && !buf.isEmpty()
                && !ctx.getBroadcastState(SCHEDULE).immutableEntries().iterator().hasNext()) {
            // The schedule hasn't arrived yet (job just started and the two sources race).
            // Judging headways now would mark them all UNSCHEDULED, so wait.
            ctx.timerService().registerEventTimeTimer(ts + SCHEDULE_WAIT_MS);
            return;
        }
        if (buf != null && !buf.isEmpty()) {
            ArrayList<Arrival> ready = new ArrayList<>();
            ArrayList<Arrival> keep = new ArrayList<>();
            for (Arrival a : buf) {
                (a.arrivalTs + releaseDelayMs <= ts ? ready : keep).add(a);
            }
            ready.sort(Comparator.comparingLong(a -> a.arrivalTs));
            for (Arrival a : ready) {
                release(a, ctx, out);
            }
            if (keep.isEmpty()) {
                pending.clear();
            } else {
                pending.update(keep);
            }
        }

        Long gt = gapTimer.value();
        if (gt != null && gt == ts && openGapId.value() == null) {
            Arrival prev = lastReleased.value();
            if (trainAlreadyCame(pending.value(), prev, ts)) {
                // The next train got here before the deadline. It's only waiting for its
                // release, so the stop isn't overdue.
                return;
            }
            Integer sched = lookup(ctx.getBroadcastState(SCHEDULE), prev.routeId, prev.stopId, ts);
            if (sched != null) { // no scheduled service now means no train is overdue
                Incident inc = incident("GAP_OPENED", prev, null, ts, null, sched, gapThreshold.value());
                openGapId.update(inc.incidentId);
                gaps.inc();
                ctx.output(INCIDENTS, inc);
            }
        }
    }

    private void release(Arrival a, OnTimerContext ctx, Collector<Headway> out) throws Exception {
        Arrival prev = lastReleased.value();
        if (prev != null && a.arrivalTs < prev.arrivalTs) {
            // A later train at this stop was already processed. Counting it now would
            // produce a negative or misleading headway, so it's only counted.
            lateArrivals.inc();
            return;
        }
        if (prev != null && a.tripKey.equals(prev.tripKey)) {
            return;
        }
        ReadOnlyBroadcastState<String, Integer> schedule = ctx.getBroadcastState(SCHEDULE);
        if (prev != null) {
            long headwayS = (a.arrivalTs - prev.arrivalTs) / 1000;
            Integer sched = lookup(schedule, a.routeId, a.stopId, a.arrivalTs);
            // Gaps are judged on the threshold set when the previous train arrived (the
            // same one the gap timer uses), so the headway's status and the incidents agree
            // across an hour boundary where the schedule changes.
            Integer gapThr = gapTimer.value() == null ? null : gapThreshold.value();
            String status = HeadwayRules.classify(headwayS, sched);
            if ("GAP".equals(status) && (gapThr == null || headwayS < gapThr)) {
                status = "OK";
            } else if ("OK".equals(status) && gapThr != null && headwayS >= gapThr) {
                status = "GAP";
            }
            long now = System.currentTimeMillis();

            Headway h = new Headway();
            h.routeId = a.routeId;
            h.direction = a.direction;
            h.stopId = a.stopId;
            h.tripKey = a.tripKey;
            h.prevTripKey = prev.tripKey;
            h.arrivalTs = a.arrivalTs;
            h.prevArrivalTs = prev.arrivalTs;
            h.headwaySeconds = headwayS;
            h.scheduledHeadwaySeconds = sched;
            h.status = status;
            h.detectedAt = now;
            out.collect(h);

            if (sched == null) {
                unscheduled.inc();
            }
            String openId = openGapId.value();
            if (openId != null) {
                ctx.output(INCIDENTS, incident("GAP_CLOSED", prev, a, a.arrivalTs, headwayS,
                        sched == null ? 0 : sched, gapThreshold.value()));
            } else if ("GAP".equals(status)) {
                // The timer didn't fire first (for example the watermark jumped), so
                // open and close the gap together. Judged on the same threshold the timer
                // used, from the schedule when the previous train arrived; near an hour
                // boundary that can differ from the schedule at this train's arrival.
                Integer schedPrev = lookup(schedule, prev.routeId, prev.stopId, prev.arrivalTs);
                int sp = schedPrev == null ? 0 : schedPrev;
                ctx.output(INCIDENTS, incident("GAP_OPENED", prev, null, prev.arrivalTs + gapThr * 1000L, null, sp, gapThr));
                ctx.output(INCIDENTS, incident("GAP_CLOSED", prev, a, a.arrivalTs, headwayS, sp, gapThr));
                gaps.inc();
            }
            if ("BUNCHING".equals(status)) {
                bunching.inc();
                ctx.output(INCIDENTS, incident("BUNCHING", prev, a, a.arrivalTs, headwayS, sched,
                        HeadwayRules.bunchingThreshold(sched)));
            }
        }

        lastReleased.update(a);
        openGapId.clear();
        // The old gap timer is left to fire and ignored (gapTimer no longer matches).
        // Deleting it could also delete a release timer that shares its timestamp,
        // because Flink keeps one timer per key and timestamp.
        gapTimer.clear();
        Integer schedNow = lookup(schedule, a.routeId, a.stopId, a.arrivalTs);
        if (schedNow != null) {
            int thr = HeadwayRules.gapThreshold(schedNow);
            long t = a.arrivalTs + thr * 1000L;
            ctx.timerService().registerEventTimeTimer(t);
            gapTimer.update(t);
            gapThreshold.update(thr);
        }
    }

    static boolean trainAlreadyCame(ArrayList<Arrival> waiting, Arrival prev, long deadline) {
        if (waiting == null) {
            return false;
        }
        for (Arrival a : waiting) {
            if (a.arrivalTs > prev.arrivalTs && a.arrivalTs <= deadline && !a.tripKey.equals(prev.tripKey)) {
                return true;
            }
        }
        return false;
    }

    static Integer lookup(ReadOnlyBroadcastState<String, Integer> schedule, String route, String stop, long ts)
            throws Exception {
        return schedule.get(ScheduleRow.keyAt(route, stop, ts));
    }

    static Incident incident(String type, Arrival prev, Arrival cur, long eventTs, Long headwayS,
                             int sched, Integer threshold) {
        Incident i = new Incident();
        String kind = type.startsWith("GAP") ? "GAP" : type;
        i.incidentId = UUID.nameUUIDFromBytes((prev.routeId + "|" + prev.stopId + "|" + kind + "|" + prev.arrivalTs)
                .getBytes(StandardCharsets.UTF_8)).toString();
        i.eventType = type;
        i.routeId = prev.routeId;
        i.direction = prev.direction;
        i.stopId = prev.stopId;
        i.prevArrivalTs = prev.arrivalTs;
        i.prevTripKey = prev.tripKey;
        i.arrivalTs = cur == null ? null : cur.arrivalTs;
        i.tripKey = cur == null ? null : cur.tripKey;
        i.eventTs = eventTs;
        i.headwaySeconds = headwayS;
        i.scheduledHeadwaySeconds = sched;
        i.thresholdSeconds = threshold == null ? 0 : threshold;
        i.detectedAt = System.currentTimeMillis();
        return i;
    }
}
