"""Pydantic v2 models for every Kafka message in the pipeline.

All messages are serialized as UTF-8 JSON and carry ``schema_version``.
Timestamps are epoch seconds (float) so latency/freshness can be computed downstream.
"""

from __future__ import annotations

import hashlib
import re
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator

SCHEMA_VERSION = 1
SELFTEXT_MAX = 5_000
SNIPPET_MAX = 300
MAX_TOPICS = 8
MAX_TRENDS = 50
MAX_SAMPLE_TITLES = 3

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

Source = Literal["reddit", "replay", "synthetic"]
SentimentLabel = Literal["pos", "neu", "neg"]
EntityLabel = Literal["ORG", "PERSON", "GPE", "PRODUCT", "EVENT", "WORK_OF_ART", "NORP"]
ALLOWED_ENTITY_LABELS: frozenset[str] = frozenset(EntityLabel.__args__)  # type: ignore[attr-defined]


def hash_author(name: str | None) -> str:
    """SHA-256 a Reddit username. Raw usernames are never stored or transmitted."""
    return hashlib.sha256((name or "[deleted]").encode("utf-8")).hexdigest()


def sentiment_label(compound: float) -> SentimentLabel:
    """Standard VADER thresholds."""
    if compound >= 0.05:
        return "pos"
    if compound <= -0.05:
        return "neg"
    return "neu"


class _Message(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=False)

    schema_version: int = SCHEMA_VERSION

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes | str) -> Self:
        return cls.model_validate_json(data)


class _AuthorHashed(_Message):
    author: str

    @field_validator("author", mode="before")
    @classmethod
    def _ensure_hashed(cls, v: object) -> str:
        s = "" if v is None else str(v)
        return s if _SHA256_RE.match(s) else hash_author(s or None)


# --------------------------------------------------------------------------- reddit.raw
class RawPost(_AuthorHashed):
    id: str
    subreddit: str
    title: str
    selftext: str = ""
    score: int = 0
    num_comments: int = 0
    url: str = ""
    created_utc: float
    ingested_at: float
    source: Source

    @field_validator("selftext", mode="before")
    @classmethod
    def _truncate_selftext(cls, v: object) -> str:
        return ("" if v is None else str(v))[:SELFTEXT_MAX]


# --------------------------------------------------------------------------- reddit.enriched
class Entity(BaseModel):
    text: str
    label: EntityLabel


class EnrichedPost(_AuthorHashed):
    id: str
    subreddit: str
    title: str
    snippet: str = ""
    score: int = 0
    num_comments: int = 0
    url: str = ""
    created_utc: float
    ingested_at: float
    source: Source

    topics: list[str] = Field(default_factory=list, max_length=MAX_TOPICS)
    entities: list[Entity] = Field(default_factory=list)
    category: str
    category_confidence: float = Field(ge=0.0, le=1.0)
    sentiment: float = Field(ge=-1.0, le=1.0)
    sentiment_label: SentimentLabel
    processed_at: float
    nlp_ms: float

    @field_validator("snippet", mode="before")
    @classmethod
    def _truncate_snippet(cls, v: object) -> str:
        return ("" if v is None else str(v))[:SNIPPET_MAX]

    @classmethod
    def from_raw(cls, raw: RawPost, **enrichment: object) -> EnrichedPost:
        base = raw.model_dump(exclude={"selftext", "schema_version"})
        return cls(snippet=raw.selftext[:SNIPPET_MAX], **base, **enrichment)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- trends.updates
class TrendItem(BaseModel):
    topic: str
    category: str
    score: float
    count_window: int
    count_baseline: float
    velocity: float
    avg_sentiment: float
    sample_titles: list[str] = Field(default_factory=list, max_length=MAX_SAMPLE_TITLES)
    # Parallel to sample_titles; lets the dashboard link each title to Reddit.
    sample_urls: list[str] = Field(default_factory=list, max_length=MAX_SAMPLE_TITLES)
    subreddit_count: int = 0
    first_seen: float


class CategoryStat(BaseModel):
    category: str
    count_window: int
    avg_sentiment: float


class TrendUpdate(_Message):
    generated_at: float
    window_seconds: int
    trends: list[TrendItem] = Field(default_factory=list, max_length=MAX_TRENDS)
    category_stats: list[CategoryStat] = Field(default_factory=list)
    max_source_ingested_at: float | None = None
    total_window: int = 0


# --------------------------------------------------------------------------- reddit.dlq
class DeadLetter(_Message):
    source_topic: str
    partition: int
    offset: int
    key: str | None = None
    error: str
    payload: str
    failed_at: float
