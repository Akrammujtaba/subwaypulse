package io.subwaypulse;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Works out which stops a train has passed by comparing two consecutive updates.
 *
 * <p>The feed never says "the train arrived at X". It lists the stops still ahead. When
 * a stop drops off the front of that list, the train has served it, and the best
 * estimate of when is the last time we saw predicted for it. That prediction was made
 * moments before, while the train was at or next to the platform.
 *
 * <p>Two things are not arrivals:
 * <ul>
 *   <li>A stop that disappears while its predicted time is still well in the future.
 *       That's a reroute or a skipped stop, not a train that got there early.</li>
 *   <li>A stop that disappears from the middle of the list. Only stops ahead of the
 *       train's new first stop count as passed.</li>
 * </ul>
 */
public final class ArrivalInference {
    /** A stop dropping off more than this long before its predicted time is a skip. */
    public static final long SKIP_IF_PREDICTED_AHEAD_MS = 5 * 60_000L;

    private ArrivalInference() {}

    public static final class Result {
        public final List<StopPred> passed = new ArrayList<>();
        public int skipped;
    }

    /**
     * @param previous the stops ahead of the train at the last update, in order
     * @param current  the stops ahead of the train now, in order
     * @param feedTs   the feed timestamp of the current update (epoch ms)
     */
    public static Result passedStops(List<StopPred> previous, List<StopPred> current, long feedTs) {
        Result r = new Result();
        if (previous == null || previous.isEmpty()) {
            return r;
        }
        Set<String> stillAhead = new HashSet<>();
        for (StopPred s : current) {
            stillAhead.add(s.stopId);
        }
        int cut = previous.size();
        if (!current.isEmpty()) {
            String next = current.get(0).stopId;
            for (int i = 0; i < previous.size(); i++) {
                if (previous.get(i).stopId.equals(next)) {
                    cut = i;
                    break;
                }
            }
            // If the new first stop wasn't in the old list (a reroute), fall back to
            // the time check alone for every stop that disappeared.
        }
        for (int i = 0; i < cut; i++) {
            StopPred s = previous.get(i);
            if (stillAhead.contains(s.stopId)) {
                continue;
            }
            if (s.ts > feedTs + SKIP_IF_PREDICTED_AHEAD_MS) {
                r.skipped++;
                continue;
            }
            r.passed.add(new StopPred(s.stopId, Math.min(s.ts, feedTs)));
        }
        return r;
    }

    /** A trip left the feed. Stops predicted at or before that moment were reached. */
    public static Result reachedBeforeVanishing(List<StopPred> previous, long feedTs) {
        Result r = new Result();
        if (previous == null) {
            return r;
        }
        for (StopPred s : previous) {
            if (s.ts <= feedTs) {
                r.passed.add(new StopPred(s.stopId, s.ts));
            }
        }
        return r;
    }
}
