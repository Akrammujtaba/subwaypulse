package io.subwaypulse;

import io.subwaypulse.avro.TripEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Keyed by feed, fed only heartbeats. The collector sends one after every successful
 * fetch, so the time between two heartbeats is the time the feed went unobserved. A
 * long silence (the feed was down, the collector stopped, or the machine slept) is
 * emitted as an {@link Outage}, so HeadwayMonitor doesn't read "no data" as "no trains".
 *
 * <p>Every new heartbeat also goes out as a "seen" marker. An outage on one feed is only
 * known once that feed comes back, while the other feeds keep event time moving. So
 * HeadwayMonitor doesn't open a gap until the feed has been seen past the deadline: by
 * then, any outage covering the deadline has been reported, because the outage is
 * emitted just before the marker of the heartbeat that ends it.
 */
public class OutageDetector extends KeyedProcessFunction<String, TripEvent, Reference> {
    private final long maxSilenceMs;
    private transient ValueState<Long> lastSeen;
    private transient Counter outages;

    public OutageDetector(long maxSilenceMs) {
        this.maxSilenceMs = maxSilenceMs;
    }

    @Override
    public void open(Configuration parameters) {
        lastSeen = getRuntimeContext().getState(new ValueStateDescriptor<>("lastSeen", Types.LONG));
        outages = getRuntimeContext().getMetricGroup().counter("feedOutages");
    }

    @Override
    public void processElement(TripEvent hb, Context ctx, Collector<Reference> out) throws Exception {
        long ts = hb.getFeedTs().toEpochMilli();
        Long last = lastSeen.value();
        if (last != null && ts <= last) {
            return; // the same heartbeat via another partition, or an older one
        }
        if (last != null && ts - last > maxSilenceMs) {
            outages.inc();
            out.collect(Reference.of(new Outage(hb.getFeedId(), last, ts)));
        }
        lastSeen.update(ts);
        out.collect(Reference.seen(hb.getFeedId(), ts));
    }
}
