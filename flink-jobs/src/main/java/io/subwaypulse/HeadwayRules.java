package io.subwaypulse;

/**
 * Same thresholds as subwaypulse/rules.py (the Python reference used by the simulator
 * and evaluator). tests/test_rules_parity.py runs both over one table of cases.
 */
public final class HeadwayRules {
    public static final int BUNCH_FLOOR_S = 90;
    public static final double BUNCH_FRACTION = 0.30;
    public static final int GAP_EXTRA_FLOOR_S = 300;

    private HeadwayRules() {}

    public static int bunchingThreshold(int scheduledS) {
        return Math.max(BUNCH_FLOOR_S, (int) (BUNCH_FRACTION * scheduledS));
    }

    public static int gapThreshold(int scheduledS) {
        return scheduledS + Math.max(GAP_EXTRA_FLOOR_S, scheduledS);
    }

    /** BUNCHING, GAP, OK, or UNSCHEDULED when there is no schedule to compare to. */
    public static String classify(long headwayS, Integer scheduledS) {
        if (scheduledS == null || scheduledS <= 0) {
            return "UNSCHEDULED";
        }
        if (headwayS <= bunchingThreshold(scheduledS)) {
            return "BUNCHING";
        }
        if (headwayS >= gapThreshold(scheduledS)) {
            return "GAP";
        }
        return "OK";
    }

    /** CLI used by the parity test: reads "headway scheduled" pairs, prints a label per line. */
    public static void main(String[] args) throws Exception {
        java.io.BufferedReader in = new java.io.BufferedReader(new java.io.InputStreamReader(System.in));
        String line;
        while ((line = in.readLine()) != null) {
            String[] p = line.trim().split("\\s+");
            if (p.length < 2) {
                continue;
            }
            Integer sched = p[1].equals("-") ? null : Integer.valueOf(p[1]);
            System.out.println(classify(Long.parseLong(p[0]), sched));
        }
    }
}
