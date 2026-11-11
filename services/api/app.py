"""FastAPI service: REST snapshot + WebSocket push of trend updates.

- ``GET  /health``          Kafka + Redis reachability
- ``GET  /api/trends``      current TrendUpdate (from Redis ``trends:latest``); ``?category=``
- ``GET  /api/categories``  the 15 categories with window stats
- ``GET  /api/metrics``     aggregated live pipeline metrics (Redis ``metrics:*``)
- ``WS   /ws/trends``       snapshot on connect, then every TrendUpdate; ``?category=`` filter
- ``WS   /ws/posts``        lightweight stream of newly enriched posts (dashboard ticker)

Every pushed frame is ``{"type": ..., "server_sent_at": <epoch s>, "data": {...}}``.
Heartbeat ``{"type": "ping"}`` every 15 s of silence.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from services.api.broadcaster import Broadcaster
from services.producer.synthetic import load_taxonomy
from shared.config import (
    REDIS_METRICS_PREFIX,
    REDIS_TRENDS_KEY,
    TOPIC_ENRICHED,
    TOPIC_TRENDS,
    Settings,
    get_settings,
)
from shared.kafka import make_consumer
from shared.log import get_logger
from shared.metrics import Metrics
from shared.schemas import EnrichedPost, TrendUpdate

HEARTBEAT_S = 15.0
POST_FIELDS = ("id", "subreddit", "title", "url", "category", "sentiment", "sentiment_label",
               "topics", "ingested_at", "processed_at")
log = get_logger("api")


def filter_update(update: dict[str, Any], category: str | None) -> dict[str, Any]:
    if not category:
        return update
    return {**update, "trends": [t for t in update.get("trends", []) if t["category"] == category]}


def empty_update(window_s: int) -> dict[str, Any]:
    return {"schema_version": 1, "generated_at": None, "window_seconds": window_s, "trends": [],
            "category_stats": [], "max_source_ingested_at": None, "total_window": 0}


def frame(kind: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"type": kind, "server_sent_at": time.time()}
    if data is not None:
        out["data"] = data
    return out


def summarize_metrics(services: dict[str, list[dict[str, Any]]], latest: dict[str, Any] | None,
                      now: float) -> dict[str, Any]:
    def rate(svc_prefix: str, counter: str) -> float:
        return sum(i["rates_per_s"].get(counter, 0.0) for name, insts in services.items()
                   if name.startswith(svc_prefix) for i in insts)

    def total(svc_prefix: str, counter: str) -> float:
        return sum(i["counters"].get(counter, 0.0) for name, insts in services.items()
                   if name.startswith(svc_prefix) for i in insts)

    workers = services.get("nlp_worker", [])
    hists = [w["histograms"].get("ingest_latency_s") for w in workers]
    hists = [h for h in hists if h and h["n"]]
    latency = {k: round(max(h[k] for h in hists) * 1000, 2) for k in ("p50", "p95", "p99")} \
        if hists else {"p50": 0.0, "p95": 0.0, "p99": 0.0}
    api = services.get("api", [{}])[0] if services.get("api") else {}
    e2e = api.get("histograms", {}).get("e2e_s") if api else None
    freshness = None
    if latest and latest.get("max_source_ingested_at"):
        freshness = round(now - latest["max_source_ingested_at"], 3)
    return {
        "produced_per_s": round(rate("producer_", "posts_produced"), 2),
        "produced_total": int(total("producer_", "posts_produced")),
        "enriched_per_s": round(rate("nlp_worker", "posts_enriched"), 2),
        "enriched_total": int(total("nlp_worker", "posts_enriched")),
        "dlq_total": int(total("nlp_worker", "dlq")),
        "nlp_workers": len(workers),
        "consumer_lag": int(sum(w["gauges"].get("consumer_lag", 0) for w in workers)),
        "ingest_latency_ms": latency,
        "trend_updates_per_s": round(rate("trend_engine", "updates_emitted"), 2),
        "posts_in_window": latest.get("total_window") if latest else 0,
        "ws_clients": int(api.get("gauges", {}).get("ws_clients", 0)) if api else 0,
        "e2e_p95_ms": round(e2e["p95"] * 1000, 1) if e2e and e2e["n"] else None,
        "freshness_s": freshness,
        "projected_per_day": int(rate("producer_", "posts_produced") * 86_400),
    }


class ApiState:
    def __init__(self, settings: Settings, redis: Any) -> None:
        self.settings = settings
        self.redis = redis
        self.trends = Broadcaster(max_queue=16)
        self.posts = Broadcaster(max_queue=64)
        self.latest: dict[str, Any] | None = None
        self.metrics = Metrics("api", hist_size=300)  # ~5 min of 1/s updates
        self.kafka_ok = False
        self.tasks: list[asyncio.Task] = []
        self.consumers: list[Any] = []
        self.categories = list(load_taxonomy())

    # -- Kafka -> broadcaster ---------------------------------------------
    def on_trend_update(self, raw: bytes) -> None:
        update = TrendUpdate.from_bytes(raw).model_dump()
        self.latest = update
        now = time.time()
        if update.get("max_source_ingested_at"):
            self.metrics.observe("e2e_s", now - update["max_source_ingested_at"])
        self.trends.publish(update)
        self.metrics.inc("updates_broadcast")

    def on_enriched(self, raw: bytes) -> None:
        if not self.posts.subscribers:
            return
        post = EnrichedPost.from_bytes(raw)
        self.posts.publish({k: getattr(post, k) for k in POST_FIELDS})

    async def consume(self, topic: str, handler: Callable[[bytes], None]) -> None:
        consumer = make_consumer(topic, group_id=None, auto_offset_reset="latest")
        self.consumers.append(consumer)
        while True:
            try:
                await consumer.start()
                self.kafka_ok = True
                break
            except Exception as exc:  # noqa: BLE001 - keep retrying until Kafka is up
                self.kafka_ok = False
                log.warning("kafka not ready", extra={"topic": topic, "error": repr(exc)})
                await asyncio.sleep(2)
        async for msg in consumer:
            try:
                handler(msg.value)
            except Exception as exc:  # noqa: BLE001
                log.warning("bad message", extra={"topic": topic, "error": repr(exc)})

    async def snapshot(self) -> dict[str, Any] | None:
        try:
            raw = await self.redis.get(REDIS_TRENDS_KEY)
        except Exception:  # noqa: BLE001 - fall back to in-memory copy
            raw = None
        if raw:
            return TrendUpdate.from_bytes(raw).model_dump()
        return self.latest


def create_app(settings: Settings | None = None, redis_client: Any = None,
               start_kafka: bool = True) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        redis = redis_client or aioredis.from_url(settings.redis_url)
        state = ApiState(settings, redis)
        app.state.api = state
        stop = asyncio.Event()
        state.tasks.append(asyncio.create_task(
            state.metrics.publish_forever(redis, settings.metrics_interval_s, stop)))
        if start_kafka:
            state.tasks.append(asyncio.create_task(state.consume(TOPIC_TRENDS, state.on_trend_update)))
            state.tasks.append(asyncio.create_task(state.consume(TOPIC_ENRICHED, state.on_enriched)))
        log.info("api started", extra={"kafka": start_kafka})
        try:
            yield
        finally:
            stop.set()
            for t in state.tasks:
                t.cancel()
            await asyncio.gather(*state.tasks, return_exceptions=True)
            for c in state.consumers:
                with contextlib.suppress(Exception):
                    await c.stop()
            if redis_client is None:
                await redis.aclose()

    app = FastAPI(title="Reddit Trending Pipeline", version="0.1.0", lifespan=lifespan)
    app.add_middleware(CORSMiddleware, allow_origins=settings.cors_origin_list,
                       allow_methods=["GET"], allow_headers=["*"])

    def st() -> ApiState:
        return app.state.api

    def check_category(category: str | None) -> str | None:
        if category and category not in st().categories:
            raise HTTPException(404, f"unknown category {category!r}")
        return category

    @app.get("/health")
    async def health() -> JSONResponse:
        s = st()
        redis_ok = False
        with contextlib.suppress(Exception):
            redis_ok = bool(await asyncio.wait_for(s.redis.ping(), timeout=2))
        kafka_ok = s.kafka_ok if start_kafka else None
        ok = redis_ok and kafka_ok is not False
        body = {"status": "ok" if ok else "degraded", "redis": redis_ok, "kafka": kafka_ok,
                "ws_clients": len(s.trends)}
        return JSONResponse(body, status_code=200 if ok else 503)

    @app.get("/api/trends")
    async def trends(category: str | None = Query(None)) -> dict[str, Any]:
        check_category(category)
        snap = await st().snapshot()
        return filter_update(snap or empty_update(settings.trend_window_s), category)

    @app.get("/api/categories")
    async def categories() -> list[dict[str, Any]]:
        snap = await st().snapshot()
        stats = {c["category"]: c for c in (snap or {}).get("category_stats", [])}
        tax = load_taxonomy()
        return [
            {"category": name, "subreddits": tax[name]["subreddits"],
             "count_window": stats.get(name, {}).get("count_window", 0),
             "avg_sentiment": stats.get(name, {}).get("avg_sentiment", 0.0)}
            for name in st().categories
        ]

    @app.get("/api/metrics")
    async def metrics() -> dict[str, Any]:
        s = st()
        services: dict[str, list[dict[str, Any]]] = {}
        keys = [k async for k in s.redis.scan_iter(match=f"{REDIS_METRICS_PREFIX}*")]
        for key in keys:
            name = (key.decode() if isinstance(key, bytes) else key)[len(REDIS_METRICS_PREFIX):]
            raw = await s.redis.hgetall(key)
            services[name] = sorted((json.loads(v) for v in raw.values()),
                                    key=lambda m: m["instance"])
        return {"generated_at": time.time(),
                "summary": summarize_metrics(services, s.latest or await s.snapshot(), time.time()),
                "services": services}

    async def _pump(ws: WebSocket, broadcaster: Broadcaster, kind: str,
                    category: str | None, initial: dict[str, Any] | None) -> None:
        s = st()
        sub = broadcaster.subscribe(category)
        s.metrics.set("ws_clients", len(s.trends) + len(s.posts))

        async def reader() -> None:  # detects disconnects; client messages are ignored
            while True:
                await ws.receive_text()

        read_task = asyncio.create_task(reader())
        try:
            if initial is not None:
                await ws.send_json(frame("snapshot", filter_update(initial, category)))
            while not read_task.done():
                get = asyncio.create_task(sub.queue.get())
                done, _ = await asyncio.wait({get, read_task}, timeout=HEARTBEAT_S,
                                             return_when=asyncio.FIRST_COMPLETED)
                if get in done:
                    data = get.result()
                    await ws.send_json(frame(kind, filter_update(data, category)
                                             if kind == "update" else data))
                    continue
                get.cancel()
                if read_task in done:
                    break
                await ws.send_json(frame("ping"))
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            read_task.cancel()
            broadcaster.unsubscribe(sub)
            s.metrics.set("ws_clients", len(s.trends) + len(s.posts))

    @app.websocket("/ws/trends")
    async def ws_trends(ws: WebSocket, category: str | None = None) -> None:
        if category and category not in st().categories:
            await ws.close(code=1008, reason="unknown category")
            return
        await ws.accept()
        initial = await st().snapshot() or empty_update(settings.trend_window_s)
        await _pump(ws, st().trends, "update", category, initial)

    @app.websocket("/ws/posts")
    async def ws_posts(ws: WebSocket) -> None:
        await ws.accept()
        await _pump(ws, st().posts, "post", None, None)

    return app
