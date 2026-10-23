"""Service rules: when a headway counts as bunching or a gap.

This is the reference version used by the simulator's ground truth and by the
evaluator. The Flink job implements the same thresholds in
`flink-jobs/.../HeadwayRules.java`, and `tests/test_rules_parity.py` checks the two
agree on a shared table of cases.

Thresholds scale with how often trains are scheduled, because a 4-minute wait is a
problem at 8 AM on the L and normal at 3 AM:

- BUNCHING: the train arrived within max(90s, 30% of the scheduled headway) of the
  one ahead. Riders see two trains together, then a long wait.
- GAP: no train for the scheduled headway plus max(5 min, one more scheduled
  headway). On a 4-minute schedule that is 9 minutes; on a 20-minute schedule, 40.

These are my thresholds, not MTA's official performance metrics.
"""

from __future__ import annotations

BUNCH_FLOOR_S = 90
BUNCH_FRACTION = 0.30
GAP_EXTRA_FLOOR_S = 300


def bunching_threshold_s(scheduled_headway_s: int) -> int:
    return max(BUNCH_FLOOR_S, int(BUNCH_FRACTION * scheduled_headway_s))


def gap_threshold_s(scheduled_headway_s: int) -> int:
    return scheduled_headway_s + max(GAP_EXTRA_FLOOR_S, scheduled_headway_s)


def classify(headway_s: float, scheduled_headway_s: int | None) -> str | None:
    """Return 'BUNCHING', 'GAP', or None. Unknown schedule means no judgement."""
    if scheduled_headway_s is None or scheduled_headway_s <= 0:
        return None
    if headway_s <= bunching_threshold_s(scheduled_headway_s):
        return "BUNCHING"
    if headway_s >= gap_threshold_s(scheduled_headway_s):
        return "GAP"
    return None
