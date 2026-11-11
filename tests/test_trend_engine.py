import random

import pytest

from services.trend_engine.engine import Candidate, TrendEngine, merge_near_duplicates
from shared.schemas import EnrichedPost

CATS = ["Technology", "AI/ML", "Gaming", "Sports", "Politics", "World News", "Finance/Crypto",
        "Science", "Health", "Entertainment", "Music", "Business/Startups", "Education/Careers",
        "Lifestyle", "Memes/Humor"]
BACKGROUND = [(f"topic {i}", CATS[i % 15]) for i in range(60)]


def post(topics, ts, category="Technology", sentiment=0.0, sub="technology", pid=None):
    return EnrichedPost(
        id=pid or f"p{ts}{topics[0]}", subreddit=sub, title=f"about {topics[0]}", author="a",
        created_utc=ts, ingested_at=ts, source="synthetic", topics=topics, category=category,
        category_confidence=0.9, sentiment=sentiment, sentiment_label="neu",
        processed_at=ts + 0.1, nlp_ms=1.0,
    )


def feed_background(engine, rng, t_start, t_end, rate):
    t = t_start
    step = 1 / rate
    while t < t_end:
        topic, cat = rng.choice(BACKGROUND)
        engine.add(post([topic], t, cat, sentiment=rng.uniform(-1, 1), sub=f"sub{rng.randint(0, 5)}"))
        t += step


def test_basic_counts_and_category_stats():
    e = TrendEngine(CATS, min_count=1)
    for i in range(5):
        e.add(post(["bitcoin"], 1000 + i, "Finance/Crypto", sentiment=0.5))
    snap = e.snapshot(now=1010)
    assert snap.trends[0].topic == "bitcoin" and snap.trends[0].count_window == 5
    assert snap.trends[0].avg_sentiment == 0.5
    fin = next(c for c in snap.category_stats if c.category == "Finance/Crypto")
    assert fin.count_window == 5 and len(snap.category_stats) == 15
    assert snap.max_source_ingested_at == 1004


def test_min_count_gate():
    e = TrendEngine(CATS, min_count=3)
    e.add(post(["rare"], 1000))
    e.add(post(["rare"], 1001))
    assert e.snapshot(now=1005).trends == []
    e.add(post(["rare"], 1002))
    assert [t.topic for t in e.snapshot(now=1005).trends] == ["rare"]


def test_window_excludes_old_posts_and_baseline_normalizes():
    e = TrendEngine(CATS, window_s=300, baseline_s=3600, min_count=1, started_at=0)
    # steady 1 mention / 10s for an hour -> baseline expected ~30 per 5 min window
    for t in range(0, 3900, 10):
        e.add(post(["steady"], t))
    snap = e.snapshot(now=3899)
    item = snap.trends[0]
    assert 28 <= item.count_window <= 31
    assert 28 <= item.count_baseline <= 31
    assert abs(item.score) < 3  # not trending: in line with its baseline


def test_spike_ranks_first_within_two_emit_cycles():
    """One average-popularity topic spikes to 10x its rate.

    Background: 20 posts/s spread over 60 topics (~0.33/s each) for 30 min, so every topic has a
    baseline. The target topic shares that baseline, then runs at 10x (~3.3/s) for 30 s of event
    time. Emits happen every 1 s; once the spike's posts are ingested the topic must be #1 within
    2 emit cycles, and detection while the spike is in progress must take well under 30 s.
    """
    rng = random.Random(0)
    e = TrendEngine(CATS, window_s=300, baseline_s=3600, min_count=3, started_at=0)
    rate = 20.0
    per_topic = rate / len(BACKGROUND)
    t = 0.0
    while t < 1800:
        feed_background(e, rng, t, t + 1, rate)
        if rng.random() < per_topic:
            e.add(post(["quantum toaster"], t + 0.5))
        t += 1
    assert e.snapshot(now=t).trends[0].topic != "quantum toaster"

    spike_rate = 10 * per_topic
    first_top = None
    for sec in range(30):
        feed_background(e, rng, t, t + 1, rate)
        n = int(spike_rate) + (1 if rng.random() < spike_rate % 1 else 0)
        for k in range(n):
            e.add(post(["quantum toaster"], t + (k + 0.5) / n, sub=f"sub{rng.randint(0, 9)}"))
        t += 1
        if e.snapshot(now=t).trends[0].topic == "quantum toaster" and first_top is None:
            first_top = sec + 1
    print(f"\n10x spike detected as #1 after {first_top}s of spike")
    assert first_top is not None and first_top < 20, first_top
    ranks = []
    for _ in range(2):  # the next 2 emit cycles
        feed_background(e, rng, t, t + 1, rate)
        t += 1
        ranks.append(e.snapshot(now=t).trends[0].topic)
    assert ranks[-1] == "quantum toaster" and ranks[0] == "quantum toaster"


def test_large_injected_spike_is_top_after_one_cycle():
    """Freshness-benchmark scenario: a brand-new topic injected at 20/s on top of 20/s background."""
    rng = random.Random(1)
    e = TrendEngine(CATS, window_s=300, baseline_s=3600, min_count=3, started_at=0)
    feed_background(e, rng, 0, 900, 20)
    for k in range(20):  # 1 s of injection
        e.add(post(["zorblax launch"], 900 + k * 0.05, sub=f"sub{k % 4}"))
        feed_background(e, rng, 900 + k * 0.05, 900 + (k + 1) * 0.05, 20)
    top = [x.topic for x in e.snapshot(now=901.0).trends[:10]]
    assert top[0] == "zorblax launch"


def test_velocity():
    e = TrendEngine(CATS, min_count=1, started_at=0)
    for t in range(940, 970, 3):  # previous 30s slice: 10 posts
        e.add(post(["x"], t))
    for t in range(970, 1000, 1):  # last 30s slice: 30 posts
        e.add(post(["x"], t))
    item = e.snapshot(now=999).trends[0]
    assert item.velocity == 20


def test_merge_near_duplicates():
    def c(topic, score, count, cat="Gaming"):
        return Candidate(topic, cat, score, count, 0, 0, 0, {"s"}, [(topic, "u")], 0)

    merged = merge_near_duplicates([
        c("gta 6", 10, 40), c("gta 6 release date", 8, 30), c("the lakers", 7, 25, "Sports"),
        c("lakers", 6, 20, "Sports"), c("laker", 5, 18, "Sports"), c("switch", 4, 12, "Technology"),
        c("nintendo switch", 3, 10, "Gaming"),
    ])
    names = [m.topic for m in merged]
    assert "gta 6 release date" not in names and "gta 6" in names
    assert "the lakers" in names and "lakers" not in names and "laker" not in names
    # different categories are not merged by containment
    assert "switch" in names and "nintendo switch" in names
    assert merged[0].count_window == 40 and len(merged[0].samples) == 2


def test_memory_flat_over_long_replay():
    """Simulate 3h at 5 posts/s with constantly new one-off topics; tracked state must plateau."""
    rng = random.Random(2)
    e = TrendEngine(CATS, window_s=300, baseline_s=3600, min_count=3, started_at=0)
    sizes = []
    uid = 0
    for minute in range(180):
        t0 = minute * 60
        for k in range(300):
            uid += 1
            topic = rng.choice(BACKGROUND)[0] if rng.random() < 0.7 else f"one-off {uid}"
            e.add(post([topic], t0 + k * 0.2, pid=str(uid)))
        e.snapshot(now=t0 + 60)
        sizes.append((len(e.topics), e.bucket_count()))
    # after the 65-min horizon fills, size should stop growing
    plateau = sizes[70:]
    assert max(s[0] for s in plateau) <= 1.1 * min(s[0] for s in plateau)
    assert max(s[1] for s in plateau) <= 1.1 * min(s[1] for s in plateau)


def test_should_emit():
    e = TrendEngine(CATS)
    assert not e.should_emit(101.0, 100.5)
    e.add(post(["a"], 100))
    assert e.should_emit(100.5, 100.0)
    e.snapshot(now=100.5)
    assert not e.should_emit(101.0, 100.5)
    assert e.should_emit(110.1, 109.9)  # crossed a bucket boundary


def test_top_50_and_samples_capped():
    e = TrendEngine(CATS, min_count=1)
    for i in range(80):
        for k in range(i % 7 + 1):
            e.add(post([f"t{i}"], 1000 + k, pid=f"{i}-{k}"))
    snap = e.snapshot(now=1010)
    assert len(snap.trends) == 50
    assert all(len(t.sample_titles) <= 3 for t in snap.trends)


@pytest.mark.parametrize("bad", [(305, 3600, 10), (300, 3605, 10)])
def test_invalid_config(bad):
    with pytest.raises(ValueError):
        TrendEngine(CATS, *bad)
