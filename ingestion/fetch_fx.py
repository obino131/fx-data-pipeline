"""
Fetches latest FX rates from the Frankfurter API and loads them into the bronze layer.
Entry point for manual runs and (later) Airflow orchestration.
"""
import sys
import requests

from ingestion.config import config
from ingestion.logger import get_logger
from ingestion.db import init_schema, insert_rates

logger = get_logger(__name__)


def fetch_latest_rates() -> dict:
    """
    Calls Frankfurter's /latest endpoint.
    Raises requests.HTTPError on non-2xx responses.
    """
    url = f"{config.FX_API_BASE_URL}/latest"
    params = {
        "base": config.FX_BASE_CURRENCY,
        "symbols": config.FX_TARGET_CURRENCIES,
    }
    logger.info(f"Requesting rates: base={config.FX_BASE_CURRENCY}, symbols={config.FX_TARGET_CURRENCIES}")

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def parse_rates(payload: dict) -> list[tuple]:
    """
    Converts the API JSON payload into a list of tuples ready for insertion.
    Expected payload shape: {"base": "EUR", "date": "2026-07-06", "rates": {"USD": 1.08, ...}}
    """
    if "rates" not in payload or "base" not in payload:
        raise ValueError(f"Unexpected API response shape: {payload}")

    base = payload["base"]
    rate_date = payload["date"]
    rows = []
    for target_currency, rate in payload["rates"].items():
        rows.append((base, target_currency, rate, rate_date, "frankfurter"))
    return rows


def run():
    logger.info("Starting FX ingestion run.")
    init_schema()

    try:
        payload = fetch_latest_rates()
    except requests.RequestException as e:
        logger.error(f"FX API request failed: {e}")
        sys.exit(1)

    try:
        rows = parse_rates(payload)
    except ValueError as e:
        logger.error(f"Failed to parse API response: {e}")
        sys.exit(1)

    if not rows:
        logger.warning("No rate rows parsed from API response — nothing to insert.")
        return

    insert_rates(rows)
    logger.info("FX ingestion run completed successfully.")


if __name__ == "__main__":
    run()
