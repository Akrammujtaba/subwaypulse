package io.subwaypulse;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class ArrivalInferenceTest {
    static StopPred p(String stop, long sec) {
        return new StopPred(stop, sec * 1000);
    }

    static List<String> ids(ArrivalInference.Result r) {
        return r.passed.stream().map(s -> s.stopId).toList();
    }

    @Test
    void stopsThatDropOffTheFrontArePassed() {
        var prev = List.of(p("A01N", 100), p("A02N", 220), p("A03N", 340));
        var cur = List.of(p("A03N", 345));
        var r = ArrivalInference.passedStops(prev, cur, 250_000);
        assertEquals(List.of("A01N", "A02N"), ids(r));
        assertEquals(220_000, r.passed.get(1).ts);
    }

    @Test
    void arrivalIsNeverAfterTheUpdateThatRevealedIt() {
        // Train was predicted at 300 but the stop was already gone at 280.
        var r = ArrivalInference.passedStops(List.of(p("A01N", 300), p("A02N", 400)), List.of(p("A02N", 390)), 280_000);
        assertEquals(280_000, r.passed.get(0).ts);
    }

    @Test
    void aStopDroppedFarAheadOfItsTimeIsASkipNotAnArrival() {
        var prev = List.of(p("A01N", 100), p("A02N", 1000), p("A03N", 1200));
        var cur = List.of(p("A03N", 1200));
        // At t=110 the train passed A01N; A02N vanished 15 minutes early: rerouted.
        var r = ArrivalInference.passedStops(prev, cur, 110_000);
        assertEquals(List.of("A01N"), ids(r));
        assertEquals(1, r.skipped);
    }

    @Test
    void stopsMissingFromTheMiddleAreNotPassed() {
        var prev = List.of(p("A01N", 100), p("A02N", 200), p("A03N", 300));
        var cur = List.of(p("A01N", 105), p("A03N", 300));
        var r = ArrivalInference.passedStops(prev, cur, 90_000);
        assertEquals(List.of(), ids(r));
    }

    @Test
    void noHistoryMeansNoArrivals() {
        assertEquals(List.of(), ids(ArrivalInference.passedStops(null, List.of(p("A01N", 1)), 0)));
    }

    @Test
    void vanishedTripReachedOnlyStopsDueByThen() {
        var r = ArrivalInference.reachedBeforeVanishing(List.of(p("A20N", 500), p("A21N", 700)), 600_000);
        assertEquals(List.of("A20N"), ids(r));
    }
}
