"""Sliding-window trend detection (pure; every method takes ``now`` explicitly for testability).

Counts are kept per topic in fixed ``bucket_s`` buckets (default 10 s), keyed by the post's
``ingested_at``.

- current window  = buckets within the last ``window_s`` (default 5 min)
- baseline        = the ``baseline_s`` before that (default 60 min), normalized to window length
- expected        = baseline_rate * window_s
- z               = (count_window - expected) / sqrt(expected + 1)
- velocity        = count(last 30 s) - count(previous 30 s)
- z_velocity      = max(velocity, 0) / sqrt(expected_30s + 1)
- score           = (z + z_velocity) * (1 + SPREAD_BOOST * min(distinct_subreddits - 1, SPREAD_CAP))

Topics with fewer than ``min_count`` mentions in the window are never reported. Buckets older than
the baseline horizon are dropped, so memory is bounded by (topics active in the last ~65 min).
"""

from __future__ import annotations

import math
import re
from collections import Counter, deque
from dataclasses import dataclass, field

from shared.schemas import (
    MAX_SAMPLE_TITLES,
    MAX_TRENDS,
    CategoryStat,
    EnrichedPost,
    TrendItem,
    TrendUpdate,
)

SLICE_S = 30
SPREAD_BOOST = 0.05
SPREAD_CAP = 6
MIN_BASELINE_S = 60  # less observed history than this => no baseline (expected = 0)
MERGE_POOL = 200  # only the top-N candidates are considered for near-duplicate merging


@dataclass
class Bucket:
    count: int = 0
    sentiment_sum: float = 0.0
    subreddits: set[str] = field(default_factory=set)
    categories: Counter[str] = field(default_factory=Counter)


@dataclass
class TopicState:
    first_seen: float
    buckets: dict[int, Bucket] = field(default_factory=dict)
    samples: deque[tuple[str, str]] = field(default_factory=lambda: deque(maxlen=MAX_SAMPLE_TITLES))


@dataclass
class Candidate:
    topic: str
    category: str
    score: float
    count_window: int
    expected: float
    velocity: float
    avg_sentiment: float
    subreddits: set[str]
    samples: list[tuple[str, str]]
    first_seen: float


def _words(phrase: str) -> tuple[str, ...]:
    return tuple(re.findall(r"[\w'+#.-]+", phrase.lower()))


def _stem_key(phrase: str) -> tuple[str, ...]:
    """Crude lemma key so 'laker'/'lakers', 'the switch 2'/'switch 2' collapse."""
    return tuple(w[:-1] if len(w) > 3 and w.endswith("s") else w for w in _words(phrase))


def _contains(longer: tuple[str, ...], shorter: tuple[str, ...]) -> bool:
    n = len(shorter)
    return 0 < n < len(longer) and any(longer[i : i + n] == shorter for i in range(len(longer) - n + 1))


def merge_near_duplicates(cands: list[Candidate]) -> list[Candidate]:
    """Merge phrases with the same stem key, or contained in another of the same category.

    Candidates must be sorted by score (desc). The highest-scoring member names the group; the
    group's count is the max member count (members usually co-occur in the same posts, so summing
    would double count).
    """
    kept: list[Candidate] = []
    keys: list[tuple[str, ...]] = []
    for cand in cands:
        key = _stem_key(cand.topic)
        target = None
        for i, (k, existing) in enumerate(zip(keys, kept, strict=True)):
            if k == key or (existing.category == cand.category and (_contains(k, key) or _contains(key, k))):
                target = i
                break
        if target is None:
            kept.append(cand)
            keys.append(key)
            continue
        g = kept[target]
        g.count_window = max(g.count_window, cand.count_window)
        g.subreddits |= cand.subreddits
        g.first_seen = min(g.first_seen, cand.first_seen)
        seen_titles = {t for t, _ in g.samples}
        for s in cand.samples:
            if len(g.samples) < MAX_SAMPLE_TITLES and s[0] not in seen_titles:
                g.samples.append(s)
    return kept


class TrendEngine:
    def __init__(
        self, categories: list[str], window_s: int = 300, baseline_s: int = 3600,
        bucket_s: int = 10, min_count: int = 3, started_at: float | None = None,
    ) -> None:
        if window_s % bucket_s or baseline_s % bucket_s or SLICE_S % bucket_s:
            raise ValueError("window, baseline and 30s slice must be multiples of bucket_s")
        self.categories = list(categories)
        self.window_s, self.baseline_s, self.bucket_s = window_s, baseline_s, bucket_s
        self.min_count = min_count
        self.window_n = window_s // bucket_s
        self.baseline_n = baseline_s // bucket_s
        self.slice_n = SLICE_S // bucket_s
        self.topics: dict[str, TopicState] = {}
        self.cat_buckets: dict[str, dict[int, list[float]]] = {c: {} for c in self.categories}
        self.started_at = started_at
        self.max_ingested_at: float | None = None
        self.posts_seen = 0
        self.dirty = False
        self._last_pruned_idx: int | None = None

    # ------------------------------------------------------------------ ingest
    def _idx(self, ts: float) -> int:
        return int(ts // self.bucket_s)

    def add(self, post: EnrichedPost) -> None:
        ts = post.ingested_at
        if self.started_at is None or ts < self.started_at:
            self.started_at = ts
        idx = self._idx(ts)
        for topic in post.topics:
            st = self.topics.get(topic)
            if st is None:
                st = self.topics[topic] = TopicState(first_seen=ts)
            b = st.buckets.get(idx)
            if b is None:
                b = st.buckets[idx] = Bucket()
            b.count += 1
            b.sentiment_sum += post.sentiment
            b.subreddits.add(post.subreddit)
            b.categories[post.category] += 1
            st.samples.append((post.title, post.url))
        if post.category in self.cat_buckets:
            cb = self.cat_buckets[post.category].setdefault(idx, [0, 0.0])
            cb[0] += 1
            cb[1] += post.sentiment
        if self.max_ingested_at is None or ts > self.max_ingested_at:
            self.max_ingested_at = ts
        self.posts_seen += 1
        self.dirty = True

    # ------------------------------------------------------------------ housekeeping
    def prune(self, now: float) -> int:
        """Drop buckets older than window + baseline. Returns number of topics removed."""
        horizon = self._idx(now) - self.window_n - self.baseline_n
        removed = 0
        for topic in list(self.topics):
            st = self.topics[topic]
            for idx in [i for i in st.buckets if i <= horizon]:
                del st.buckets[idx]
            if not st.buckets:
                del self.topics[topic]
                removed += 1
        for buckets in self.cat_buckets.values():
            for idx in [i for i in buckets if i <= horizon]:
                del buckets[idx]
        self._last_pruned_idx = self._idx(now)
        return removed

    def bucket_count(self) -> int:
        return sum(len(st.buckets) for st in self.topics.values())

    # ------------------------------------------------------------------ scoring
    def _observed_baseline_s(self, now: float) -> float:
        if self.started_at is None:
            return 0.0
        observed = now - self.window_s - self.started_at
        return max(0.0, min(float(self.baseline_s), observed))

    def candidates(self, now: float) -> list[Candidate]:
        cur = self._idx(now)
        win_lo = cur - self.window_n  # window = (win_lo, cur]
        base_lo = win_lo - self.baseline_n  # baseline = (base_lo, win_lo]
        s1_lo = cur - self.slice_n  # last slice = (s1_lo, cur]
        s0_lo = s1_lo - self.slice_n  # previous slice = (s0_lo, s1_lo]
        observed = self._observed_baseline_s(now)
        out: list[Candidate] = []
        for topic, st in self.topics.items():
            cw = 0
            sent = 0.0
            base = s1 = s0 = 0
            subs: set[str] = set()
            cats: Counter[str] = Counter()
            for idx, b in st.buckets.items():
                if idx > cur:
                    continue
                if idx > win_lo:
                    cw += b.count
                    sent += b.sentiment_sum
                    subs |= b.subreddits
                    cats.update(b.categories)
                    if idx > s1_lo:
                        s1 += b.count
                    elif idx > s0_lo:
                        s0 += b.count
                elif idx > base_lo:
                    base += b.count
            if cw < self.min_count:
                continue
            expected = base / observed * self.window_s if observed >= MIN_BASELINE_S else 0.0
            expected_slice = expected * SLICE_S / self.window_s
            velocity = float(s1 - s0)
            z = (cw - expected) / math.sqrt(expected + 1)
            z_vel = max(velocity, 0.0) / math.sqrt(expected_slice + 1)
            spread = 1 + SPREAD_BOOST * min(len(subs) - 1, SPREAD_CAP)
            out.append(Candidate(
                topic=topic, category=cats.most_common(1)[0][0], score=(z + z_vel) * spread,
                count_window=cw, expected=expected, velocity=velocity,
                avg_sentiment=sent / cw, subreddits=subs, samples=list(st.samples)[::-1],
                first_seen=st.first_seen,
            ))
        out.sort(key=lambda c: (-c.score, -c.count_window, c.topic))
        return out

    def category_stats(self, now: float) -> list[CategoryStat]:
        cur = self._idx(now)
        win_lo = cur - self.window_n
        stats = []
        for cat in self.categories:
            n = 0
            s = 0.0
            for idx, (cnt, ssum) in self.cat_buckets[cat].items():
                if win_lo < idx <= cur:
                    n += int(cnt)
                    s += ssum
            stats.append(CategoryStat(category=cat, count_window=n,
                                      avg_sentiment=round(s / n, 4) if n else 0.0))
        return stats

    def snapshot(self, now: float, top_n: int = MAX_TRENDS) -> TrendUpdate:
        if self._last_pruned_idx != self._idx(now):
            self.prune(now)
        cands = self.candidates(now)
        merged = merge_near_duplicates(cands[:MERGE_POOL])[:top_n]
        cat_stats = self.category_stats(now)
        self.dirty = False
        return TrendUpdate(
            generated_at=now,
            window_seconds=self.window_s,
            trends=[
                TrendItem(
                    topic=c.topic, category=c.category, score=round(c.score, 3),
                    count_window=c.count_window, count_baseline=round(c.expected, 3),
                    velocity=c.velocity, avg_sentiment=round(c.avg_sentiment, 4),
                    sample_titles=[t for t, _ in c.samples[:MAX_SAMPLE_TITLES]],
                    sample_urls=[u for _, u in c.samples[:MAX_SAMPLE_TITLES]],
                    subreddit_count=len(c.subreddits), first_seen=c.first_seen,
                )
                for c in merged
            ],
            category_stats=cat_stats,
            max_source_ingested_at=self.max_ingested_at,
            total_window=sum(s.count_window for s in cat_stats),
        )

    def should_emit(self, now: float, last_emit_at: float | None) -> bool:
        """Emit if new posts arrived, or a bucket boundary passed (scores age even without input)."""
        if self.dirty:
            return True
        if last_emit_at is None:
            return False
        return self._idx(now) != self._idx(last_emit_at)
