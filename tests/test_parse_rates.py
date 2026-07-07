"""
Unit test for the FX response parsing logic.
Tests parse_rates() in isolation — no network, no database.
"""
import pytest
from ingestion.fetch_fx import parse_rates


def test_parse_rates_returns_correct_tuples():
    payload = {
        "base": "EUR",
        "date": "2026-07-06",
        "rates": {"USD": 1.08, "GBP": 0.85},
    }
    result = parse_rates(payload)

    assert len(result) == 2
    assert ("EUR", "USD", 1.08, "2026-07-06", "frankfurter") in result
    assert ("EUR", "GBP", 0.85, "2026-07-06", "frankfurter") in result


def test_parse_rates_raises_on_missing_rates_key():
    payload = {"base": "EUR", "date": "2026-07-06"}
    with pytest.raises(ValueError):
        parse_rates(payload)


def test_parse_rates_empty_rates_returns_empty_list():
    payload = {"base": "EUR", "date": "2026-07-06", "rates": {}}
    result = parse_rates(payload)
    assert result == []
