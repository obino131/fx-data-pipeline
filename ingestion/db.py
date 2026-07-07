"""
Database connection and schema management for the bronze layer.
"""
import psycopg2
from ingestion.config import config
from ingestion.logger import get_logger

logger = get_logger(__name__)

CREATE_BRONZE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS bronze_fx_rates (
    id SERIAL PRIMARY KEY,
    base_currency VARCHAR(3) NOT NULL,
    target_currency VARCHAR(3) NOT NULL,
    rate NUMERIC(18, 6) NOT NULL,
    rate_date DATE NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
    source VARCHAR(50) NOT NULL DEFAULT 'frankfurter'
);
"""

def get_connection():
    return psycopg2.connect(config.db_dsn)

def init_schema():
    """Creates the bronze table if it doesn't exist. Safe to run repeatedly."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_BRONZE_TABLE_SQL)
        conn.commit()
    logger.info("Bronze schema verified/created.")

def insert_rates(rows: list[tuple]):
    """
    Inserts a batch of rate rows.
    Each row: (base_currency, target_currency, rate, rate_date, source)
    """
    insert_sql = """
        INSERT INTO bronze_fx_rates (base_currency, target_currency, rate, rate_date, source)
        VALUES (%s, %s, %s, %s, %s);
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows)
        conn.commit()
    logger.info(f"Inserted {len(rows)} rows into bronze_fx_rates.")
