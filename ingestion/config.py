"""
Centralized configuration for the ingestion layer.
Reads all values from environment variables (.env), never hardcodes secrets.
"""
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    POSTGRES_USER = os.environ["POSTGRES_USER"]
    POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
    POSTGRES_DB = os.environ["POSTGRES_DB"]
    POSTGRES_HOST = os.environ["POSTGRES_HOST"]
    POSTGRES_PORT = os.environ["POSTGRES_PORT"]

    FX_API_BASE_URL = "https://api.frankfurter.dev/v1"
    FX_BASE_CURRENCY = os.environ.get("FX_BASE_CURRENCY", "EUR")
    FX_TARGET_CURRENCIES = os.environ.get("FX_TARGET_CURRENCIES", "USD,GBP,CHF,JPY")

    @property
    def db_dsn(self) -> str:
        return (
            f"host={self.POSTGRES_HOST} "
            f"port={self.POSTGRES_PORT} "
            f"dbname={self.POSTGRES_DB} "
            f"user={self.POSTGRES_USER} "
            f"password={self.POSTGRES_PASSWORD}"
        )

config = Config()
