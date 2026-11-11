import time

import fakeredis

from shared.metrics import Metrics, percentile


def test_percentile_nearest_rank():
    vals = list(range(1, 101))
    assert percentile(vals, 50) == 50 and percentile(vals, 95) == 95 and percentile(vals, 100) == 100
    assert percentile([], 95) == 0.0


def test_histogram_reports_recent_window_only(monkeypatch):
    m = Metrics("svc", instance="i", hist_window_s=60)
    t0 = 1_000_000.0
    monkeypatch.setattr(time, "time", lambda: t0)
    m.observe("lat", 300.0)  # old backlog sample
    monkeypatch.setattr(time, "time", lambda: t0 + 120)
    m.observe("lat", 0.1)
    snap = m.snapshot()
    assert snap["histograms"]["lat"]["n"] == 1 and snap["histograms"]["lat"]["p95"] == 0.1


def test_rates_and_counters():
    m = Metrics("svc", instance="i")
    m.inc("x", 10)
    snap = m.snapshot()
    assert snap["counters"]["x"] == 10 and snap["rates_per_s"]["x"] > 0


async def test_publish_and_cleanup_on_stop():
    import asyncio

    r = fakeredis.FakeAsyncRedis()
    m = Metrics("svc", instance="i")
    stop = asyncio.Event()
    task = asyncio.create_task(m.publish_forever(r, 0.01, stop))
    await asyncio.sleep(0.05)
    assert await r.hexists("metrics:svc", "i")
    stop.set()
    await task
    assert not await r.hexists("metrics:svc", "i")
