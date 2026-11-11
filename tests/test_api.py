import asyncio
import json
import time

import fakeredis
import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from services.api import app as api_module
from services.api.app import create_app, summarize_metrics
from services.api.broadcaster import Broadcaster
from shared.config import REDIS_TRENDS_KEY
from shared.metrics import Metrics
from shared.schemas import CategoryStat, TrendItem, TrendUpdate


def make_update(ts=None, topics=(("gpt-6", "AI/ML"), ("switch 2", "Gaming"))) -> TrendUpdate:
    now = ts or time.time()
    return TrendUpdate(
        generated_at=now, window_seconds=300,
        trends=[TrendItem(topic=t, category=c, score=10 - i, count_window=20, count_baseline=1,
                          velocity=3, avg_sentiment=0.1, sample_titles=[f"{t} news"],
                          sample_urls=["https://www.reddit.com/x"], first_seen=now - 60)
                for i, (t, c) in enumerate(topics)],
        category_stats=[CategoryStat(category="AI/ML", count_window=20, avg_sentiment=0.1)],
        max_source_ingested_at=now - 0.4, total_window=40,
    )


@pytest.fixture
def redis():
    return fakeredis.FakeAsyncRedis()


@pytest.fixture
def client(redis):
    app = create_app(redis_client=redis, start_kafka=False)
    with TestClient(app) as c:
        yield c


def seed(client, redis, update: TrendUpdate):
    client.portal.call(redis.set, REDIS_TRENDS_KEY, update.to_bytes())


def push(client, update: TrendUpdate):
    """Simulate the Kafka consumer receiving a TrendUpdate (runs on the app's event loop)."""
    client.portal.call(client.app.state.api.on_trend_update, update.to_bytes())


# ---------------------------------------------------------------- REST
def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["redis"] is True


def test_trends_empty_then_snapshot(client, redis):
    assert client.get("/api/trends").json()["trends"] == []
    seed(client, redis, make_update())
    body = client.get("/api/trends").json()
    assert [t["topic"] for t in body["trends"]] == ["gpt-6", "switch 2"]


def test_trends_category_filter(client, redis):
    seed(client, redis, make_update())
    body = client.get("/api/trends", params={"category": "Gaming"}).json()
    assert [t["topic"] for t in body["trends"]] == ["switch 2"]
    assert client.get("/api/trends", params={"category": "Nope"}).status_code == 404


def test_categories_has_15(client, redis):
    seed(client, redis, make_update())
    cats = client.get("/api/categories").json()
    assert len(cats) == 15
    ai = next(c for c in cats if c["category"] == "AI/ML")
    assert ai["count_window"] == 20 and ai["subreddits"]


def test_metrics_aggregates_services(client, redis):
    w = Metrics("nlp_worker", instance="w1")
    w.inc("posts_enriched", 100)
    for v in (0.1, 0.2, 0.3):
        w.observe("ingest_latency_s", v)
    p = Metrics("producer_synthetic", instance="p1")
    p.inc("posts_produced", 120)
    client.portal.call(w.publish, redis)
    client.portal.call(p.publish, redis)
    body = client.get("/api/metrics").json()
    assert {"nlp_worker", "producer_synthetic"} <= set(body["services"])
    s = body["summary"]
    assert s["enriched_total"] == 100 and s["produced_total"] == 120
    assert s["nlp_workers"] == 1 and s["ingest_latency_ms"]["p95"] == 300.0


def test_summarize_metrics_handles_empty():
    s = summarize_metrics({}, None, time.time())
    assert s["enriched_per_s"] == 0 and s["freshness_s"] is None


# ---------------------------------------------------------------- WebSocket
def test_ws_sends_snapshot_then_pushes_updates(client, redis):
    seed(client, redis, make_update())
    with client.websocket_connect("/ws/trends") as ws:
        first = ws.receive_json()
        assert first["type"] == "snapshot" and first["data"]["trends"][0]["topic"] == "gpt-6"
        assert first["server_sent_at"] > 0
        push(client, make_update(topics=(("bitcoin", "Finance/Crypto"),)))
        msg = ws.receive_json()
        assert msg["type"] == "update" and msg["data"]["trends"][0]["topic"] == "bitcoin"
        assert msg["server_sent_at"] >= msg["data"]["generated_at"]


def test_ws_category_filter(client, redis):
    seed(client, redis, make_update())
    with client.websocket_connect("/ws/trends?category=Gaming") as ws:
        assert [t["topic"] for t in ws.receive_json()["data"]["trends"]] == ["switch 2"]
        push(client, make_update())
        assert [t["topic"] for t in ws.receive_json()["data"]["trends"]] == ["switch 2"]


def test_ws_unknown_category_rejected(client):
    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect("/ws/trends?category=Nope") as ws:
            ws.receive_json()
    assert exc.value.code == 1008


def test_ws_disconnect_unsubscribes(client, redis):
    seed(client, redis, make_update())
    with client.websocket_connect("/ws/trends") as ws:
        ws.receive_json()
        assert len(client.app.state.api.trends) == 1
    for _ in range(50):
        if len(client.app.state.api.trends) == 0:
            break
        time.sleep(0.02)
    assert len(client.app.state.api.trends) == 0


def test_ws_heartbeat(client, redis, monkeypatch):
    monkeypatch.setattr(api_module, "HEARTBEAT_S", 0.05)
    with client.websocket_connect("/ws/trends") as ws:
        assert ws.receive_json()["type"] == "snapshot"
        assert ws.receive_json()["type"] == "ping"


def test_ws_posts_stream(client):
    from services.producer.synthetic import SyntheticGenerator
    from shared.schemas import EnrichedPost

    raw = SyntheticGenerator(seed=1).make_post()
    enriched = EnrichedPost.from_raw(raw, topics=["x"], category="Gaming", category_confidence=0.9,
                                     sentiment=0.2, sentiment_label="pos", processed_at=time.time(),
                                     nlp_ms=1.0)
    with client.websocket_connect("/ws/posts") as ws:
        for _ in range(50):
            if client.app.state.api.posts.subscribers:
                break
            time.sleep(0.01)
        client.portal.call(client.app.state.api.on_enriched, enriched.to_bytes())
        msg = ws.receive_json()
        assert msg["type"] == "post" and msg["data"]["id"] == raw.id
        assert "author" not in msg["data"]


# ---------------------------------------------------------------- broadcaster
async def test_broadcaster_drops_oldest_for_slow_clients():
    b = Broadcaster(max_queue=3)
    slow = b.subscribe()
    for i in range(10):
        b.publish({"i": i})
    assert slow.queue.qsize() == 3 and slow.dropped == 7
    assert [slow.queue.get_nowait()["i"] for _ in range(3)] == [7, 8, 9]


async def test_broadcaster_fanout():
    b = Broadcaster()
    subs = [b.subscribe() for _ in range(5)]
    b.publish({"x": 1})
    assert all(s.queue.get_nowait() == {"x": 1} for s in subs)
    b.unsubscribe(subs[0])
    assert len(b) == 4
    await asyncio.sleep(0)


def test_frames_are_json_serializable(client, redis):
    seed(client, redis, make_update())
    with client.websocket_connect("/ws/trends") as ws:
        json.dumps(ws.receive_json())
