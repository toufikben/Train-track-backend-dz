import sys
from datetime import datetime, timezone
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "engines"))
from observation_validation import Observation, ValidationContext, validate
from wait_decision import WaitInput, decide

def test_bad_accuracy_rejected():
    obs = Observation("1","s","t","tr",36.75,3.05,500.0,5.0,90.0,datetime.now(timezone.utc))
    r = validate(obs, ValidationContext(50,0.8,0.8,None,None))
    assert r.accepted is False or r.score < 0.5

def test_wait_runs():
    w = decide(WaitInput(None, None, "HIGH", "LIVE", None))
    assert w.decision.value in ("UNCERTAIN", "CONSIDER_ALTERNATIVE", "WAIT")

if __name__ == "__main__":
    test_bad_accuracy_rejected()
    test_wait_runs()
    print("tests OK")
