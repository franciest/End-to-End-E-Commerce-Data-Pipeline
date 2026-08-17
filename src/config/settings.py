"""
config/settings.py

Loads configuration from environment variables (populated from .env via
python-dotenv) into a single typed Settings object. Import `settings` from
here everywhere instead of calling os.getenv() scattered across the codebase
— one place to see every config value the project depends on, and it fails
fast at import time if something required is missing.

Usage:
    from config.settings import settings
    engine = create_engine(settings.postgres_url)
"""

from dataclasses import dataclass
import os

from dotenv import load_dotenv

# Loads variables from a .env file in the project root into the process
# environment. No-op if the variables are already set (e.g. inside Docker,
# where docker-compose already injects them) — safe to call in both contexts.
load_dotenv()


def _require(key: str) -> str:
    """Fetch an env var or raise immediately with a clear error, instead of
    letting a missing config value surface later as a confusing connection
    failure deep inside some other module."""
    value = os.getenv(key)
    if value is None or value == "":
        raise RuntimeError(
            f"Missing required environment variable: {key}. "
            f"Check your .env file against .env.example."
        )
    return value


@dataclass(frozen=True)
class Settings:
    # ---- Postgres ----
    postgres_user: str
    postgres_password: str
    postgres_db: str
    postgres_host: str
    postgres_port: str

    # ---- MinIO ----
    minio_endpoint: str
    minio_root_user: str
    minio_root_password: str
    minio_bucket: str

    # ---- Kafka ----
    kafka_broker: str
    kafka_topic_orders: str

    # ---- Simulated source API ----
    api_base_url: str

    @property
    def postgres_url(self) -> str:
        """SQLAlchemy-style connection string for the warehouse DB."""
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


def load_settings() -> Settings:
    return Settings(
        postgres_user=_require("POSTGRES_USER"),
        postgres_password=_require("POSTGRES_PASSWORD"),
        postgres_db=_require("POSTGRES_DB"),
        postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
        postgres_port=os.getenv("POSTGRES_PORT", "5432"),
        minio_endpoint=_require("MINIO_ENDPOINT"),
        minio_root_user=_require("MINIO_ROOT_USER"),
        minio_root_password=_require("MINIO_ROOT_PASSWORD"),
        minio_bucket=_require("MINIO_BUCKET"),
        kafka_broker=os.getenv("KAFKA_BROKER", "localhost:9092"),
        kafka_topic_orders=os.getenv("KAFKA_TOPIC_ORDERS", "orders_stream"),
        api_base_url=os.getenv("API_BASE_URL", "http://localhost:8000"),
    )


# Import this singleton elsewhere: `from config.settings import settings`
settings = load_settings()
