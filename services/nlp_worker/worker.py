"""NLP worker: ``reddit.raw`` -> spaCy enrichment -> ``reddit.enriched``.

Micro-batching: a batch is flushed when it reaches ``NLP_BATCH_SIZE`` messages **or** when
``NLP_FLUSH_MS`` has elapsed since the first message in the batch arrived — whichever comes first.
Offsets are committed only after every enriched record in the batch has been acknowledged by
Kafka (at-least-once). Undecodable / failing messages go to ``reddit.dlq``.

Run several replicas in the ``nlp-workers`` consumer group to scale out across the 6 partitions.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord

from services.nlp_worker.enrich import Enricher
from shared.config import TOPIC_DLQ, TOPIC_ENRICHED, TOPIC_RAW, Settings
from shared.kafka import install_shutdown_handler, make_consumer, make_producer
from shared.log import get_logger
from shared.metrics import Metrics
from shared.schemas import DeadLetter, RawPost

GROUP_ID = "nlp-workers"
log = get_logger("nlp_worker")


@dataclass
class BatchBuffer:
    """Accumulates records; tells the loop when to flush (size OR age)."""

    max_size: int
    max_age_s: float
    records: list[ConsumerRecord] = field(default_factory=list)
    first_at: float | None = None

    def add(self, recs: list[ConsumerRecord], now: float) -> None:
        if recs and self.first_at is None:
            self.first_at = now
        self.records.extend(recs)

    def room(self) -> int:
        return max(1, self.max_size - len(self.records))

    def wait_ms(self, now: float) -> int:
        if self.first_at is None:
            return int(self.max_age_s * 1000)
        return max(0, int((self.first_at + self.max_age_s - now) * 1000))

    def ready(self, now: float) -> bool:
        if not self.records:
            return False
        return len(self.records) >= self.max_size or now - (self.first_at or now) >= self.max_age_s - 1e-6

    def drain(self) -> list[ConsumerRecord]:
        out, self.records, self.first_at = self.records, [], None
        return out


class NlpWorker:
    def __init__(self, settings: Settings, enricher: Enricher | None = None) -> None:
        self.settings = settings
        self.enricher = enricher or Enricher.from_settings(settings)
        self.metrics = Metrics("nlp_worker")
        self.stop = asyncio.Event()
        self.consumer: AIOKafkaConsumer | None = None
        self.producer: AIOKafkaProducer | None = None

    async def _dead_letter(self, rec: ConsumerRecord, error: str) -> None:
        assert self.producer is not None
        dl = DeadLetter(
            source_topic=rec.topic, partition=rec.partition, offset=rec.offset,
            key=rec.key.decode(errors="replace") if rec.key else None, error=error[:2000],
            payload=(rec.value or b"").decode(errors="replace")[:10_000], failed_at=time.time(),
        )
        await self.producer.send(TOPIC_DLQ, key=rec.key, value=dl.to_bytes())
        self.metrics.inc("dlq")
        log.warning("sent to dlq", extra={"post_id": dl.key, "error": dl.error[:200]})

    async def process_batch(self, records: list[ConsumerRecord]) -> int:
        assert self.producer is not None and self.consumer is not None
        posts: list[RawPost] = []
        post_recs: list[ConsumerRecord] = []
        for rec in records:
            try:
                posts.append(RawPost.from_bytes(rec.value))
                post_recs.append(rec)
            except Exception as exc:  # noqa: BLE001
                await self._dead_letter(rec, f"decode: {exc!r}")

        # spaCy is CPU-bound; run it off the event loop so heartbeats/sends keep flowing.
        results = await asyncio.to_thread(self.enricher.enrich, posts) if posts else []

        futures = []
        for rec, result in zip(post_recs, results, strict=True):
            if isinstance(result, Exception):
                await self._dead_letter(rec, f"enrich: {result!r}")
                continue
            futures.append(await self.producer.send(
                TOPIC_ENRICHED, key=result.id.encode(), value=result.to_bytes()
            ))
            self.metrics.observe("ingest_latency_s", result.processed_at - result.ingested_at)
            self.metrics.observe("nlp_ms", result.nlp_ms)
        if futures:
            await asyncio.gather(*futures)
        await self.consumer.commit()

        n = len(futures)
        self.metrics.inc("posts_enriched", n)
        self.metrics.observe("batch_size", len(records))
        return n

    async def _update_lag(self) -> None:
        """Consumer lag = sum over assigned partitions of (high watermark - position)."""
        assert self.consumer is not None
        lag = 0
        assignment = self.consumer.assignment()
        for tp in assignment:
            hw = self.consumer.highwater(tp)
            if hw is None:
                continue
            try:
                lag += max(0, hw - await self.consumer.position(tp))
            except Exception:  # noqa: BLE001 - lag is best-effort
                pass
        self.metrics.set("consumer_lag", lag)
        self.metrics.set("assigned_partitions", len(assignment))

    async def run(self) -> None:
        s = self.settings
        install_shutdown_handler(self.stop)
        redis = aioredis.from_url(s.redis_url)
        self.producer = make_producer()
        self.consumer = make_consumer(
            TOPIC_RAW, group_id=GROUP_ID, auto_offset_reset="earliest",
            max_poll_records=s.nlp_batch_size,
        )
        await self.producer.start()
        await self.consumer.start()
        metrics_task = asyncio.create_task(
            self.metrics.publish_forever(redis, s.metrics_interval_s, self.stop)
        )
        buf = BatchBuffer(max_size=s.nlp_batch_size, max_age_s=s.nlp_flush_ms / 1000)
        last_log = time.time()
        log.info("nlp worker started", extra={"model": s.spacy_model, "batch": s.nlp_batch_size,
                                              "flush_ms": s.nlp_flush_ms})
        try:
            while not self.stop.is_set():
                fetched = await self.consumer.getmany(
                    timeout_ms=buf.wait_ms(time.monotonic()), max_records=buf.room()
                )
                buf.add([r for recs in fetched.values() for r in recs], time.monotonic())
                if buf.ready(time.monotonic()):
                    await self.process_batch(buf.drain())
                    await self._update_lag()
                if time.time() - last_log >= 60:
                    snap = self.metrics.snapshot()
                    log.info("throughput", extra={
                        "enriched_total": int(snap["counters"].get("posts_enriched", 0)),
                        "latency_p95_s": round(snap["histograms"].get("ingest_latency_s", {}).get("p95", 0), 4),
                    })
                    last_log = time.time()
            if buf.records:  # graceful shutdown: finish what we hold, then commit
                await self.process_batch(buf.drain())
        finally:
            self.stop.set()
            await self.producer.flush()
            await self.consumer.stop()
            await self.producer.stop()
            await asyncio.gather(metrics_task, return_exceptions=True)
            await redis.aclose()
            log.info("nlp worker stopped")


def main(settings: Any = None) -> None:
    from shared.config import get_settings

    asyncio.run(NlpWorker(settings or get_settings()).run())
