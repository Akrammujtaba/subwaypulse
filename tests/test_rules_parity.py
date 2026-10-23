"""The Flink job and the Python evaluator must classify headways the same way,
or the evaluation would score the pipeline against different rules.

Runs the compiled Java HeadwayRules over a table of cases. Skipped unless
`mvn -f flink-jobs compile` has been run (CI's java job does)."""

import shutil
import subprocess
from pathlib import Path

import pytest

from subwaypulse.rules import classify

CLASSES = Path(__file__).resolve().parent.parent / "flink-jobs" / "target" / "classes"

CASES = [
    (60, 240), (90, 240), (91, 240), (300, 240), (539, 240), (540, 240), (2000, 240),
    (359, 1200), (360, 1200), (361, 1200), (2399, 1200), (2400, 1200),
    (30, 60), (360, 60), (361, 60), (100, None), (5000, None),
]


def python_label(headway, sched):
    if sched is None:
        return "UNSCHEDULED"
    return classify(headway, sched) or "OK"


@pytest.mark.skipif(not CLASSES.exists() or shutil.which("java") is None, reason="Java classes not built")
def test_java_and_python_rules_agree():
    stdin = "\n".join(f"{h} {'-' if s is None else s}" for h, s in CASES) + "\n"
    out = subprocess.run(["java", "-cp", str(CLASSES), "io.subwaypulse.HeadwayRules"],
                         input=stdin, capture_output=True, text=True, check=True).stdout.split()
    assert out == [python_label(h, s) for h, s in CASES]
