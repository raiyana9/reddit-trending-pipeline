import json
import time

import pytest
from pydantic import ValidationError

from shared.schemas import (
    SELFTEXT_MAX,
    SNIPPET_MAX,
    CategoryStat,
    EnrichedPost,
    Entity,
    RawPost,
    TrendItem,
    TrendUpdate,
    hash_author,
    sentiment_label,
)


def make_raw(**kw) -> RawPost:
    base = dict(
        id="abc123",
        subreddit="technology",
        title="New laptop chips announced",
        selftext="Some body text",
        author="some_user",
        score=42,
        num_comments=7,
        url="https://reddit.com/r/technology/comments/abc123",
        created_utc=time.time() - 5,
        ingested_at=time.time(),
        source="replay",
    )
    base.update(kw)
    return RawPost(**base)


def test_raw_roundtrip():
    raw = make_raw()
    again = RawPost.from_bytes(raw.to_bytes())
    assert again == raw
    assert again.schema_version == 1


def test_author_is_hashed_and_stable():
    raw = make_raw(author="spez")
    assert raw.author == hash_author("spez")
    assert len(raw.author) == 64
    assert "spez" not in raw.to_bytes().decode()
    # already-hashed values are passed through unchanged (no double hashing)
    assert RawPost.from_bytes(raw.to_bytes()).author == raw.author


def test_deleted_author():
    assert make_raw(author=None).author == hash_author(None)


def test_selftext_truncated():
    raw = make_raw(selftext="x" * (SELFTEXT_MAX + 500))
    assert len(raw.selftext) == SELFTEXT_MAX


def test_invalid_source_rejected():
    with pytest.raises(ValidationError):
        make_raw(source="twitter")


def test_enriched_from_raw_roundtrip():
    raw = make_raw(selftext="y" * 1000)
    enriched = EnrichedPost.from_raw(
        raw,
        topics=["laptop chip", "apple"],
        entities=[Entity(text="Apple", label="ORG")],
        category="Technology",
        category_confidence=0.81,
        sentiment=0.4,
        sentiment_label="pos",
        processed_at=time.time(),
        nlp_ms=3.2,
    )
    assert len(enriched.snippet) == SNIPPET_MAX
    assert "selftext" not in json.loads(enriched.to_bytes())
    assert enriched.ingested_at == raw.ingested_at
    assert EnrichedPost.from_bytes(enriched.to_bytes()) == enriched


def test_enriched_validation():
    raw = make_raw()
    kw = dict(category="Technology", category_confidence=0.5, sentiment_label="pos",
              processed_at=time.time(), nlp_ms=1.0)
    with pytest.raises(ValidationError):
        EnrichedPost.from_raw(raw, sentiment=1.5, **kw)
    with pytest.raises(ValidationError):
        EnrichedPost.from_raw(raw, sentiment=0.1, topics=[str(i) for i in range(9)], **kw)
    with pytest.raises(ValidationError):
        EnrichedPost.from_raw(raw, sentiment=0.1, entities=[{"text": "5", "label": "CARDINAL"}], **kw)


def test_trend_update_roundtrip():
    now = time.time()
    upd = TrendUpdate(
        generated_at=now,
        window_seconds=300,
        trends=[
            TrendItem(topic="gpt-5", category="AI/ML", score=12.3, count_window=40,
                      count_baseline=2.5, velocity=8, avg_sentiment=0.2,
                      sample_titles=["a", "b"], first_seen=now - 100)
        ],
        category_stats=[CategoryStat(category="AI/ML", count_window=40, avg_sentiment=0.2)],
        max_source_ingested_at=now - 0.5,
    )
    assert TrendUpdate.from_bytes(upd.to_bytes()) == upd


def test_trend_update_limits():
    now = time.time()
    item = dict(topic="t", category="Gaming", score=1, count_window=3, count_baseline=0,
                velocity=0, avg_sentiment=0, first_seen=now)
    with pytest.raises(ValidationError):
        TrendItem(**item, sample_titles=["1", "2", "3", "4"])
    with pytest.raises(ValidationError):
        TrendUpdate(generated_at=now, window_seconds=300, trends=[TrendItem(**item)] * 51)


@pytest.mark.parametrize("score,label", [(0.6, "pos"), (0.0, "neu"), (-0.3, "neg"), (0.05, "pos")])
def test_sentiment_label(score, label):
    assert sentiment_label(score) == label
