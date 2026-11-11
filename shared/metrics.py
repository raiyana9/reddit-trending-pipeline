"""In-process counters/gauges/histograms, published to Redis.

Every service instance writes a JSON snapshot into the Redis hash ``metrics:<service>``
(field = instance id) every ``METRICS_INTERVAL_S`` seconds. The API aggregates them for
``GET /api/metrics`` and the dashboard header. Using a hash field per instance keeps
replicas (e.g. ``--scale nlp_worker=3``) from overwriting each other.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import socket
import time
from collections import defaultdict, deque
from typing import Any

from shared.config import REDIS_METRICS_PREFIX

HIST_SIZE = 5_000


def percentile(values: list[float], p: float) -> float:
    """Nearest-rank percentile; ``p`` in [0, 100]. Returns 0.0 for empty input."""
    if not values:
        return 0.0
    ordered = sorted(values)
    k = max(0, min(len(ordered) - 1, math.ceil(p / 100 * len(ordered)) - 1))
    return ordered[k]


class Metrics:
    def __init__(self, service: str, instance: str | None = None) -> None:
        self.service = service
        self.instance = instance or f"{socket.gethostname()}-{os.getpid()}"
        self.started_at = time.time()
        self.counters: dict[str, float] = defaultdict(float)
        self.gauges: dict[str, float] = {}
        self.hists: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=HIST_SIZE))
        self._prev_counters: dict[str, float] = {}
        self._prev_t = time.time()

    # -- recording ---------------------------------------------------------
    def inc(self, name: str, n: float = 1.0) -> None:
        self.counters[name] += n

    def set(self, name: str, value: float) -> None:
        self.gauges[name] = value

    def observe(self, name: str, value: float) -> None:
        self.hists[name].append(value)

    # -- reporting ---------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        now = time.time()
        dt = max(now - self._prev_t, 1e-6)
        rates = {
            k: (v - self._prev_counters.get(k, 0.0)) / dt for k, v in self.counters.items()
        }
        self._prev_counters = dict(self.counters)
        self._prev_t = now
        hists = {}
        for name, values in self.hists.items():
            vals = list(values)
            hists[name] = {
                "n": len(vals),
                "p50": percentile(vals, 50),
                "p95": percentile(vals, 95),
                "p99": percentile(vals, 99),
                "max": max(vals) if vals else 0.0,
            }
        return {
            "service": self.service,
            "instance": self.instance,
            "ts": now,
            "uptime_s": now - self.started_at,
            "counters": dict(self.counters),
            "rates_per_s": rates,
            "gauges": dict(self.gauges),
            "histograms": hists,
        }

    async def publish(self, redis: Any, ttl_s: int = 30) -> None:
        key = f"{REDIS_METRICS_PREFIX}{self.service}"
        await redis.hset(key, self.instance, json.dumps(self.snapshot()))
        await redis.expire(key, ttl_s)

    async def publish_forever(self, redis: Any, interval_s: float, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.publish(redis)
            except Exception:  # metrics must never take a service down
                pass
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval_s)
            except TimeoutError:
                pass
        try:
            await self.publish(redis)
        except Exception:
            pass
