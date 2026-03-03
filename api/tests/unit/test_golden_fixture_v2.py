"""Unit checks for coverage-v2 golden fixture metadata."""
import json
from pathlib import Path

import pytest


FIXTURE_PATH = Path(__file__).parent.parent / "fixtures" / "golden_queries_coverage_v2.json"


@pytest.mark.unit
def test_golden_fixture_exists():
    assert FIXTURE_PATH.exists(), f"Missing fixture: {FIXTURE_PATH}"


@pytest.mark.unit
def test_golden_fixture_has_150_queries():
    data = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    assert isinstance(data, list)
    assert len(data) == 150


@pytest.mark.unit
def test_golden_fixture_has_required_fields():
    data = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    for row in data:
        assert "question" in row
        assert "intent" in row
        assert "language" in row
        assert row["language"] in {"en", "es"}


@pytest.mark.unit
def test_golden_fixture_contains_core_intents():
    data = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    intents = {row["intent"] for row in data}
    required = {
        "extrema_dual",
        "bottom_n",
        "nth_rank",
        "all_periods_coverage",
        "rolling_window",
        "zero_activity",
    }
    assert required.issubset(intents)
