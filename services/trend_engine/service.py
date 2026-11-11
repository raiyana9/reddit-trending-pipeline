"""Trend engine service: ``reddit.enriched`` -> TrendEngine -> ``trends.updates`` + Redis.

On startup it rebuilds state by seeking every partition of ``reddit.enriched`` to
``now - (window + baseline)`` (``offsets_for_times``), so a restart doesn't lose the baseline.
It subscribes without a consumer group (aiokafka then assigns every partition): there is exactly one trend engine and
its state is derived from the topic itself, so committed offsets would add nothing.
"""

from __future__ import annotations

import asyncio
import time

import redis.asyncio as aioredis
from aiokafka import TopicPartition

from services.producer.synthetic import load_taxonomy
from services.trend_engine.engine import TrendEngine
from shared.config import REDIS_TRENDS_KEY, TOPIC_ENRICHED, TOPIC_TRENDS, Settings, get_settings
from shared.kafka import install_shutdown_handler, make_consumer, make_producer
from shared.log import get_logger
from shared.metrics import Metrics
from shared.schemas import EnrichedPost

log = get_logger("trend_engine")


async def _seek_to_history(consumer, lookback_s: int) -> int:
    """Wait for the (group-less) subscription to assign every partition, then seek by time."""
    tps: set[TopicPartition] = set()
    for _ in range(100):
        tps = consumer.assignment()
        if tps:
            break
        await asyncio.sleep(0.1)
    if not tps:
        raise RuntimeError(f"no partitions assigned for {TOPIC_ENRICHED} (run scripts/create_topics.py)")
    since_ms = int((time.time() - lookback_s) * 1000)
    offsets = await consumer.offsets_for_times({tp: since_ms for tp in tps})
    end = await consumer.end_offsets(list(tps))
    for tp in tps:
        found = offsets.get(tp)
        consumer.seek(tp, found.offset if found else end[tp])
    return len(tps)


async def run(settings: Settings | None = None) -> None:
    s = settings or get_settings()
    stop = asyncio.Event()
    install_shutdown_handler(stop)
    engine = TrendEngine(
        categories=list(load_taxonomy()), window_s=s.trend_window_s, baseline_s=s.trend_baseline_s,
        bucket_s=s.trend_bucket_s, min_count=s.trend_min_count,
    )
    metrics = Metrics("trend_engine")
    redis = aioredis.from_url(s.redis_url)
    producer = make_producer()
    consumer = make_consumer(TOPIC_ENRICHED, group_id=None, auto_offset_reset="latest",
                             max_poll_records=2000)
    await producer.start()
    await consumer.start()
    n_parts = await _seek_to_history(consumer, s.trend_window_s + s.trend_baseline_s)
    metrics_task = asyncio.create_task(metrics.publish_forever(redis, s.metrics_interval_s, stop))
    log.info("trend engine started", extra={"partitions": n_parts, "window_s": s.trend_window_s,
                                            "baseline_s": s.trend_baseline_s})
    last_emit: float | None = None
    next_emit = time.time()
    try:
        while not stop.is_set():
            timeout = max(0.0, next_emit - time.time())
            batches = await consumer.getmany(timeout_ms=int(timeout * 1000) or 1)
            for recs in batches.values():
                for rec in recs:
                    try:
                        engine.add(EnrichedPost.from_bytes(rec.value))
                        metrics.inc("posts_ingested")
                    except Exception as exc:  # noqa: BLE001
                        metrics.inc("bad_messages")
                        log.warning("bad enriched message", extra={"error": repr(exc)})
            now = time.time()
            if now < next_emit:
                continue
            next_emit = now + s.trend_emit_interval_s
            if not engine.should_emit(now, last_emit) and last_emit is not None:
                continue
            t0 = time.perf_counter()
            update = engine.snapshot(now)
            payload = update.to_bytes()
            await producer.send(TOPIC_TRENDS, value=payload)
            await redis.set(REDIS_TRENDS_KEY, payload)
            last_emit = now
            metrics.inc("updates_emitted")
            metrics.observe("snapshot_ms", (time.perf_counter() - t0) * 1000)
            if update.max_source_ingested_at:
                metrics.observe("ingest_to_emit_s", now - update.max_source_ingested_at)
            metrics.set("topics_tracked", len(engine.topics))
            metrics.set("buckets_tracked", engine.bucket_count())
            metrics.set("posts_in_window", update.total_window)
    finally:
        stop.set()
        await producer.flush()
        await consumer.stop()
        await producer.stop()
        await asyncio.gather(metrics_task, return_exceptions=True)
        await redis.aclose()
        log.info("trend engine stopped")


def main() -> None:
    asyncio.run(run())
