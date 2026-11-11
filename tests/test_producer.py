import asyncio
import json
import time
from types import SimpleNamespace

import pytest

from services.producer.common import PostSender, parse_duration, parse_rate
from services.producer.reddit_producer import backoff_delay, seed_subreddits, submission_to_raw
from services.producer.replay import load_posts, run_replay, run_synthetic
from services.producer.synthetic import CATALOG, SyntheticGenerator, load_taxonomy
from shared.config import ROOT, Settings
from shared.metrics import Metrics
from shared.schemas import RawPost, hash_author


class FakeProducer:
    def __init__(self):
        self.sent: list[tuple[str, bytes, bytes]] = []

    async def send(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))
        fut = asyncio.get_running_loop().create_future()
        fut.set_result(None)
        return fut

    async def flush(self):
        pass


class NullLog:
    def info(self, *a, **k): ...
    def warning(self, *a, **k): ...
    def error(self, *a, **k): ...


def make_sender():
    prod = FakeProducer()
    return PostSender(prod, Metrics("test"), NullLog()), prod


def test_taxonomy_has_exactly_15_categories():
    tax = load_taxonomy()
    assert len(tax) == 15
    assert set(tax) == set(CATALOG)


def test_parsers():
    assert parse_duration("30s") == 30
    assert parse_duration("2m") == 120
    assert parse_duration("500ms") == 0.5
    assert parse_rate("20/s") == 20
    assert parse_rate("600/m") == 10


def test_backoff_bounded():
    for attempt in range(20):
        assert 0 <= backoff_delay(attempt) <= 120


def test_seed_subreddits_default_and_override():
    subs = seed_subreddits(Settings(reddit_subreddits=""))
    assert 40 <= len(subs) <= 80
    assert seed_subreddits(Settings(reddit_subreddits="a, b")) == ["a", "b"]


def test_submission_conversion_hashes_author():
    sub = SimpleNamespace(
        id="xyz", subreddit=SimpleNamespace(display_name="python"), title="Hello",
        selftext="body", author="real_user", score=5, num_comments=1,
        permalink="/r/python/comments/xyz/hello/", created_utc=1_700_000_000.0,
    )
    raw = submission_to_raw(sub)
    assert raw.author == hash_author("real_user")
    assert raw.url == "https://www.reddit.com/r/python/comments/xyz/hello/"
    assert raw.source == "reddit"


def test_generator_is_deterministic_and_valid():
    a = SyntheticGenerator(seed=1).make_post(now=1.0)
    b = SyntheticGenerator(seed=1).make_post(now=1.0)
    assert a == b
    assert isinstance(a, RawPost) and a.title


def test_injected_post_mentions_phrase():
    post = SyntheticGenerator(seed=3).make_injected("quantum toaster", "Technology")
    assert "quantum toaster" in (post.title + post.selftext).lower()


async def test_sender_stamps_ingested_at_and_keys_by_id():
    sender, prod = make_sender()
    post = SyntheticGenerator(seed=2).make_post(now=0.0)
    before = time.time()
    await sender.send(post)
    topic, key, value = prod.sent[0]
    assert topic == "reddit.raw" and key == post.id.encode()
    assert json.loads(value)["ingested_at"] >= before


async def test_replay_fixed_rate(tmp_path):
    gen = SyntheticGenerator(seed=5)
    f = tmp_path / "p.jsonl"
    f.write_text("\n".join(gen.make_post(now=1000 + i).model_dump_json() for i in range(20)))
    sender, prod = make_sender()
    t0 = time.monotonic()
    n = await run_replay(sender, [f], asyncio.Event(), rate=200)
    assert n == 20 and len(prod.sent) == 20
    assert time.monotonic() - t0 < 1.0
    assert all(json.loads(v)["source"] == "replay" for _, _, v in prod.sent)


async def test_synthetic_injects_spike():
    sender, prod = make_sender()
    await run_synthetic(sender, asyncio.Event(), rate=50, duration_s=0.6,
                        inject_phrase="zorblax", inject_at_s=0.1, inject_rate=100,
                        inject_duration_s=0.4, seed=7)
    texts = [json.loads(v)["title"] + json.loads(v)["selftext"] for _, _, v in prod.sent]
    injected = sum("zorblax" in t.lower() for t in texts)
    assert injected >= 20
    assert len(texts) - injected >= 15


def test_committed_sample_is_valid():
    path = ROOT / "data" / "sample_posts.jsonl"
    if not path.exists():
        pytest.skip("sample not generated yet")
    posts = load_posts([path])
    assert 450 <= len(posts) <= 600
    assert len({p.id for p in posts}) == len(posts)
