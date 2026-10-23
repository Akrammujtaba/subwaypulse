package io.subwaypulse;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class HeadwayRulesTest {
    @Test
    void thresholdsScaleWithTheSchedule() {
        assertEquals(90, HeadwayRules.bunchingThreshold(240));   // floor
        assertEquals(360, HeadwayRules.bunchingThreshold(1200)); // 30%
        assertEquals(540, HeadwayRules.gapThreshold(240));       // 4 + 5 min
        assertEquals(2400, HeadwayRules.gapThreshold(1200));     // 20 + 20 min
    }

    @Test
    void classify() {
        assertEquals("BUNCHING", HeadwayRules.classify(60, 240));
        assertEquals("OK", HeadwayRules.classify(300, 240));
        assertEquals("GAP", HeadwayRules.classify(540, 240));
        assertEquals("UNSCHEDULED", HeadwayRules.classify(540, null));
    }
}
