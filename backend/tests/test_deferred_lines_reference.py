import json
from pathlib import Path


REFERENCE = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "reference"
    / "deferred_lines_reference.json"
)


def test_deferred_lines_are_explicitly_non_production() -> None:
    document = json.loads(REFERENCE.read_text(encoding="utf-8"))
    assert document["status"] == "PENDING_SERVICE_VALIDATION"
    assert document["not_live_tracking"] is True
    assert document["not_production_seed"] is True
    assert {line["android_id"] for line in document["lines"]} == {
        "thenia_tizi",
        "airport_algiers",
    }


def test_deferred_line_station_contracts_are_unique_and_ordered() -> None:
    document = json.loads(REFERENCE.read_text(encoding="utf-8"))
    for line in document["lines"]:
        stations = line["stations"]
        assert len({station["local_id"] for station in stations}) == len(stations)
        assert len({station["code"] for station in stations}) == len(stations)
        assert [station["sequence"] for station in stations] == list(
            range(1, len(stations) + 1)
        )
        assert all(station["requires_canonical_match"] for station in stations)
        assert line["publication_requirements"]
