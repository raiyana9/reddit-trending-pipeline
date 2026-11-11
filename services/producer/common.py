"""Shared producer plumbing: send a RawPost with a fresh ``ingested_at``, count it, log throughput."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from aiokafka import AIOKafkaProducer

from shared.config import TOPIC_RAW
from shared.metrics import Metrics
from shared.schemas import RawPost


class PostSender:
    """Wraps an aiokafka producer; stamps ``ingested_at`` immediately before each send."""

    def __init__(self, producer: AIOKafkaProducer, metrics: Metrics, log: Any) -> None:
        self.producer = producer
        self.metrics = metrics
        self.log = log
        self._window_count = 0
        self._window_start = time.time()

    async def send(self, post: RawPost) -> None:
        post.ingested_at = time.time()
        fut = await self.producer.send(TOPIC_RAW, key=post.id.encode(), value=post.to_bytes())
        fut.add_done_callback(self._on_delivery)
        self.metrics.inc("posts_produced")
        self.metrics.inc(f"posts_produced_{post.source}")
        self._window_count += 1

    def _on_delivery(self, fut: asyncio.Future) -> None:
        if fut.cancelled() or fut.exception() is not None:
            self.metrics.inc("produce_errors")
            self.log.error("delivery failed", extra={"error": repr(fut.exception())})

    async def log_throughput_forever(self, stop: asyncio.Event, interval_s: float = 60.0) -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval_s)
            except TimeoutError:
                pass
            now = time.time()
            elapsed = now - self._window_start
            self.log.info(
                "throughput",
                extra={
                    "posts": self._window_count,
                    "per_s": round(self._window_count / max(elapsed, 1e-6), 2),
                    "per_day_projected": int(self._window_count / max(elapsed, 1e-6) * 86_400),
                    "total": int(self.metrics.counters["posts_produced"]),
                },
            )
            self._window_count = 0
            self._window_start = now


def read_jsonl(paths: Iterable[Path]) -> Iterator[dict[str, Any]]:
    for path in paths:
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield json.loads(line)


def parse_duration(value: str) -> float:
    """'30s' -> 30, '2m' -> 120, '500ms' -> 0.5, '10' -> 10."""
    v = value.strip().lower()
    if v.endswith("ms"):
        return float(v[:-2]) / 1000
    if v.endswith("s"):
        return float(v[:-1])
    if v.endswith("m"):
        return float(v[:-1]) * 60
    return float(v)


def parse_rate(value: str) -> float:
    """'20/s' -> 20, '20' -> 20, '600/m' -> 10."""
    v = value.strip().lower()
    if v.endswith("/s"):
        return float(v[:-2])
    if v.endswith("/m"):
        return float(v[:-2]) / 60
    return float(v)
