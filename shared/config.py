"""Environment-driven configuration (pydantic-settings). Every field is documented in .env.example."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT = Path(__file__).resolve().parent.parent

TOPIC_RAW = "reddit.raw"
TOPIC_ENRICHED = "reddit.enriched"
TOPIC_TRENDS = "trends.updates"
TOPIC_DLQ = "reddit.dlq"

REDIS_TRENDS_KEY = "trends:latest"
REDIS_SEEN_KEY_PREFIX = "seen:"  # seen:<post_id>, TTL 48h
REDIS_METRICS_PREFIX = "metrics:"  # metrics:<service>


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ROOT / ".env", extra="ignore")

    kafka_bootstrap: str = "localhost:9092"
    redis_url: str = "redis://localhost:6379/0"

    reddit_client_id: str = ""
    reddit_client_secret: str = ""
    reddit_user_agent: str = "trending-pipeline/0.1 by u/unknown"
    reddit_subreddits: str = ""

    spacy_model: str = "en_core_web_sm"
    nlp_batch_size: int = 32
    nlp_flush_ms: int = 100

    trend_window_s: int = 300
    trend_baseline_s: int = 3600
    trend_emit_interval_s: float = 1.0
    trend_bucket_s: int = 10
    trend_min_count: int = 3

    api_port: int = 8000
    cors_origins: str = "http://localhost:5173,http://localhost:3000"

    log_level: str = "INFO"
    metrics_interval_s: float = 5.0

    categories_path: Path = ROOT / "config" / "categories.yaml"

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
