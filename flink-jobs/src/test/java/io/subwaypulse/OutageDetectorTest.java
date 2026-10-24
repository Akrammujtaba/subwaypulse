package io.subwaypulse;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.subwaypulse.avro.TripEvent;
import io.subwaypulse.avro.TripEventType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class OutageDetectorTest {
    static TripEvent heartbeat(long feedSec) {
        return TripEvent.newBuilder().setEventType(TripEventType.HEARTBEAT).setFeedId("l")
                .setFeedTs(Instant.ofEpochSecond(feedSec)).setFetchedAt(Instant.ofEpochSecond(feedSec))
                .setStops(List.of()).build();
    }

    @Test
    void aLongSilenceBetweenHeartbeatsIsAnOutage() throws Exception {
        var h = new KeyedOneInputStreamOperatorTestHarness<String, TripEvent, Outage>(
                new KeyedProcessOperator<>(new OutageDetector(120_000)), e -> e.getFeedId(), Types.STRING);
        h.open();
        for (long t : new long[] {0, 15, 15, 30, 45, 150, 900, 915}) { // 15 arrives twice (two partitions)
            h.processElement(heartbeat(t), t * 1000);
        }
        List<Outage> out = new ArrayList<>();
        for (Object o : h.getOutput()) {
            if (o instanceof StreamRecord<?> r) {
                out.add((Outage) r.getValue());
            }
        }
        assertEquals(1, out.size()); // 45 -> 150 is 105s, under the limit
        assertEquals(150_000, out.get(0).fromTs);
        assertEquals(900_000, out.get(0).toTs);
    }
}
