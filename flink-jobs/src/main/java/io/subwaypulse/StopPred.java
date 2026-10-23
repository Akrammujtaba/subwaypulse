package io.subwaypulse;

import java.io.Serializable;

/** The latest predicted time for one upcoming stop of one trip. */
public class StopPred implements Serializable {
    private static final long serialVersionUID = 1L;

    public String stopId;
    /** Predicted arrival, or departure when the feed gives no arrival (first stop). Epoch ms. */
    public long ts;

    public StopPred() {}

    public StopPred(String stopId, long ts) {
        this.stopId = stopId;
        this.ts = ts;
    }
}
