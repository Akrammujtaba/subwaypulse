package io.subwaypulse;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.subwaypulse.avro.StopPrediction;
import io.subwaypulse.avro.TripEvent;
import io.subwaypulse.avro.TripEventType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class ArrivalDetectorTest {
    static final String TRIP = "20261026:000600_A..N54R";

    static TripEvent update(TripEventType type, long feedSec, Object... stopsAndSecs) {
        List<StopPrediction> stops = new ArrayList<>();
        for (int i = 0; i < stopsAndSecs.length; i += 2) {
            stops.add(StopPrediction.newBuilder().setStopId((String) stopsAndSecs[i])
                    .setArrivalTs(Instant.ofEpochSecond((Integer) stopsAndSecs[i + 1])).setDepartureTs(null).build());
        }
        return TripEvent.newBuilder().setEventType(type).setFeedId("ace")
                .setFeedTs(Instant.ofEpochSecond(feedSec)).setFetchedAt(Instant.ofEpochSecond(feedSec))
                .setTripKey(TRIP).setTripId("000600_A..N54R").setStartDate("20261026").setRouteId("A")
                .setDirection("N").setStops(stops).build();
    }

    static KeyedOneInputStreamOperatorTestHarness<String, TripEvent, Arrival> harness() throws Exception {
        var h = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new ArrivalDetector()), TripEvent::getTripKey,
                org.apache.flink.api.common.typeinfo.Types.STRING);
        h.open();
        return h;
    }

    static List<String> stops(KeyedOneInputStreamOperatorTestHarness<String, TripEvent, Arrival> h) {
        List<String> out = new ArrayList<>();
        for (Object o : h.getOutput()) {
            if (o instanceof StreamRecord<?> r) {
                out.add(((Arrival) r.getValue()).stopId);
            }
        }
        return out;
    }

    @Test
    void emitsEachStopOnceEvenWhenTheTripIsResent() throws Exception {
        var h = harness();
        h.processElement(update(TripEventType.UPDATE, 100, "A01N", 110, "A02N", 230, "A03N", 350), 100_000);
        h.processElement(update(TripEventType.UPDATE, 240, "A03N", 352), 240_000);
        // Collector restarted and re-sent an older-looking but identical state
        h.processElement(update(TripEventType.UPDATE, 270, "A03N", 352), 270_000);
        assertEquals(List.of("A01N", "A02N"), stops(h));
    }

    @Test
    void trainThatBlinksOutOfTheFeedIsNotDoubleCounted() throws Exception {
        var h = harness();
        h.processElement(update(TripEventType.UPDATE, 100, "A01N", 110, "A02N", 230), 100_000);
        h.processElement(update(TripEventType.VANISHED, 130, "A01N", 110, "A02N", 230), 130_000);
        h.processElement(update(TripEventType.UPDATE, 160, "A02N", 230), 160_000);
        h.processElement(update(TripEventType.VANISHED, 260, "A02N", 230), 260_000);
        assertEquals(List.of("A01N", "A02N"), stops(h));
    }

    @Test
    void aTrainHeldAtThePlatformCountsAsArrivedBeforeItLeaves() throws Exception {
        var h = harness();
        h.processElement(update(TripEventType.UPDATE, 100, "A01N", 110, "A02N", 230), 100_000);
        // At A01N and held there: still first in the list, but the train reports STOPPED_AT
        TripEvent held = update(TripEventType.UPDATE, 130, "A01N", 112, "A02N", 400);
        held.setVehicleStatus("STOPPED_AT");
        held.setVehicleStopId("A01N");
        h.processElement(held, 130_000);
        assertEquals(List.of("A01N"), stops(h));
        // Leaving later doesn't emit it again
        h.processElement(update(TripEventType.UPDATE, 500, "A02N", 520), 500_000);
        assertEquals(List.of("A01N"), stops(h));
    }

    @Test
    void aTrainWaitingAtItsOriginTerminalHasNotArrivedAnywhere() throws Exception {
        var h = harness();
        TripEvent waiting = update(TripEventType.UPDATE, 100);
        waiting.setStops(List.of(StopPrediction.newBuilder().setStopId("A01N").setArrivalTs(null)
                .setDepartureTs(Instant.ofEpochSecond(200)).build()));
        waiting.setVehicleStatus("STOPPED_AT");
        waiting.setVehicleStopId("A01N");
        h.processElement(waiting, 100_000);
        assertEquals(List.of(), stops(h));
    }

    @Test
    void olderUpdatesForATripAreIgnored() throws Exception {
        var h = harness();
        h.processElement(update(TripEventType.UPDATE, 200, "A02N", 230), 200_000);
        h.processElement(update(TripEventType.UPDATE, 100, "A01N", 110, "A02N", 230), 100_000);
        h.processElement(update(TripEventType.UPDATE, 260, "A03N", 300), 260_000);
        assertEquals(List.of("A02N"), stops(h));
    }

    @Test
    void stateIsClearedAfterTheTripGoesQuiet() throws Exception {
        var h = harness();
        h.processElement(update(TripEventType.UPDATE, 100, "A01N", 110, "A02N", 230), 100_000);
        h.processWatermark(100_000 + ArrivalDetector.STATE_TTL_MS + 1);
        assertEquals(0, h.numKeyedStateEntries());
    }
}
