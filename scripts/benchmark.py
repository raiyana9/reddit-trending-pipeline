"""End-to-end benchmarks -> BENCHMARKS.md.

Requires the app stack (Kafka, Redis, nlp_worker(s), trend_engine, api) to be running, e.g.
``make bench`` (which starts it). No other producer should be running.

1. Throughput     synthetic load at 50 -> 100 -> 200 posts/s, 60 s each; sustained enriched rate,
                  consumer lag, extrapolated daily capacity.
2. Latency        p50/p95/p99 of ``processed_at - ingested_at`` per rate and overall.
3. Freshness      background 20 posts/s; inject a brand-new topic at 20 posts/s and time from the
                  first injected post's ``ingested_at`` until a WebSocket client sees it in the
                  top 10. 10 trials; median and max. (Preceded by a warm-up so the 5-min window
                  holds only background traffic.)

    uv run python scripts/benchmark.py              # full run (~12 min)
    uv run python scripts/benchmark.py --quick      # shorter phases for a smoke test
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import random
import statistics
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import websockets  # noqa: E402
from aiokafka import AIOKafkaConsumer  # noqa: E402

from services.producer.common import PostSender  # noqa: E402
from services.producer.synthetic import SyntheticGenerator  # noqa: E402
from shared.config import ROOT, TOPIC_ENRICHED, get_settings  # noqa: E402
from shared.kafka import make_producer  # noqa: E402
from shared.metrics import Metrics, percentile  # noqa: E402
from shared.schemas import EnrichedPost  # noqa: E402

API = os.environ.get("BENCH_API_URL", "http://localhost:8000")
WS = API.replace("http", "ws", 1) + "/ws/trends"
TARGET_FRESHNESS_S = 5.0
TARGET_P95_S = 1.0
TARGET_PER_DAY = 10_000


class _QuietLog:
    def info(self, *a, **k): ...
    def warning(self, *a, **k): ...
    def error(self, *a, **k):
        print("producer error", a, k, file=sys.stderr)


@dataclass
class Collector:
    """Records every enriched post seen on reddit.enriched: id -> (ingested_at, processed_at)."""

    records: dict[str, tuple[float, float]] = field(default_factory=dict)

    def between(self, t0: float, t1: float) -> list[tuple[float, float]]:
        return [v for v in self.records.values() if t0 <= v[0] < t1]


async def collect_enriched(col: Collector, stop: asyncio.Event) -> None:
    consumer = AIOKafkaConsumer(TOPIC_ENRICHED, bootstrap_servers=get_settings().kafka_bootstrap,
                                group_id=None, auto_offset_reset="latest", fetch_max_wait_ms=50)
    await consumer.start()
    try:
        while not stop.is_set():
            batch = await consumer.getmany(timeout_ms=200)
            for recs in batch.values():
                for r in recs:
                    e = EnrichedPost.from_bytes(r.value)
                    col.records[e.id] = (e.ingested_at, e.processed_at)
    finally:
        await consumer.stop()


def api_json(path: str) -> dict:
    with urllib.request.urlopen(f"{API}{path}", timeout=5) as r:
        return json.loads(r.read())


async def produce_at(sender: PostSender, gen: SyntheticGenerator, rate: float, seconds: float,
                     stop: asyncio.Event | None = None) -> int:
    t0 = time.monotonic()
    sent = 0
    while True:
        elapsed = time.monotonic() - t0
        if elapsed >= seconds or (stop and stop.is_set()):
            break
        while sent < rate * elapsed:
            await sender.send(gen.make_post())
            sent += 1
        await asyncio.sleep(0.004)
    await sender.producer.flush()
    return sent


# --------------------------------------------------------------------------- 1 + 2
async def throughput(sender, gen, col, rates, seconds, drain_s) -> list[dict]:
    results = []
    for rate in rates:
        lags: list[int] = []

        async def sample_lag(stop: asyncio.Event, lags: list[int] = lags) -> None:
            while not stop.is_set():
                try:
                    lags.append(int(api_json("/api/metrics")["summary"]["consumer_lag"]))
                except Exception:
                    pass
                await asyncio.sleep(2)

        print(f"  throughput @ {rate}/s for {seconds}s ...", flush=True)
        t_start = time.time()
        stop = asyncio.Event()
        lag_task = asyncio.create_task(sample_lag(stop))
        sent = await produce_at(sender, gen, rate, seconds)
        t_end = time.time()
        await asyncio.sleep(drain_s)
        stop.set()
        await lag_task
        recs = col.between(t_start, t_end)
        lat = [p - i for i, p in recs]
        # sustained rate = enriched records whose processing finished inside the load window
        done_in_window = [p for i, p in recs if p <= t_end]
        results.append({
            "rate": rate,
            "sent": sent,
            "enriched": len(recs),
            "loss": sent - len(recs),
            "sustained_per_s": len(done_in_window) / (t_end - t_start),
            "p50": percentile(lat, 50), "p95": percentile(lat, 95), "p99": percentile(lat, 99),
            "max_lag": max(lags) if lags else 0,
            "per_day": int(len(done_in_window) / (t_end - t_start) * 86_400),
        })
        r = results[-1]
        print(f"    sent={sent} enriched={len(recs)} sustained={r['sustained_per_s']:.1f}/s "
              f"p95={r['p95'] * 1000:.0f}ms max_lag={r['max_lag']}", flush=True)
    return results


# --------------------------------------------------------------------------- 3
def unique_phrase(rng: random.Random) -> str:
    syll = ["zor", "vex", "quin", "bla", "tar", "mox", "rel", "fy", "kan", "dro", "pex", "lum"]
    return "".join(rng.choice(syll) for _ in range(3)).capitalize()


async def freshness(sender, gen, trials, background_rate, inject_rate, warmup_s, cooldown_s,
                    timeout_s) -> list[dict]:
    stop_bg = asyncio.Event()
    print(f"  background {background_rate}/s; warm-up {warmup_s}s ...", flush=True)
    bg = asyncio.create_task(produce_at(sender, gen, background_rate, 10**9, stop_bg))
    await asyncio.sleep(warmup_s)
    rng = random.Random(7)
    results = []
    async with websockets.connect(WS, max_size=None) as ws:
        for n in range(trials):
            phrase = unique_phrase(rng)
            key = phrase.lower()
            injecting = asyncio.Event()
            first_ingested: list[float] = []

            async def inject(injecting: asyncio.Event = injecting, phrase: str = phrase,
                             first_ingested: list[float] = first_ingested) -> None:
                t0 = time.monotonic()
                sent = 0
                while not injecting.is_set():
                    while sent < inject_rate * (time.monotonic() - t0):
                        post = gen.make_injected(phrase, "AI/ML")
                        await sender.send(post)
                        if not first_ingested:
                            first_ingested.append(post.ingested_at)
                        sent += 1
                    await asyncio.sleep(0.004)

            # drain frames already queued so we time only fresh ones
            while True:
                try:
                    await asyncio.wait_for(ws.recv(), timeout=0.05)
                except TimeoutError:
                    break
            task = asyncio.create_task(inject())
            seen_at = None
            rank = None
            deadline = time.time() + timeout_s
            while time.time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=deadline - time.time())
                except TimeoutError:
                    break
                frame = json.loads(raw)
                if frame.get("type") not in ("update", "snapshot"):
                    continue
                top = [t["topic"] for t in frame["data"]["trends"][:10]]
                hit = next((i for i, t in enumerate(top) if key in t), None)
                if hit is not None and first_ingested:
                    seen_at = time.time()
                    rank = hit + 1
                    break
            injecting.set()
            await task
            fresh = (seen_at - first_ingested[0]) if seen_at and first_ingested else None
            results.append({"trial": n + 1, "phrase": phrase, "freshness_s": fresh, "rank": rank})
            shown = f"{fresh:.2f}s (rank {rank})" if fresh is not None else "NOT DETECTED"
            print(f"    trial {n + 1:2d} {phrase:<16} {shown}", flush=True)
            await asyncio.sleep(cooldown_s)
    stop_bg.set()
    await bg
    return results


# --------------------------------------------------------------------------- report
def machine_specs() -> dict[str, str]:
    # When run in a container (make bench), the Makefile passes the host's specs in.
    host = {k[len("BENCH_HOST_"):].lower(): v for k, v in os.environ.items()
            if k.startswith("BENCH_HOST_") and v}
    if host:
        return host
    specs = {"os": f"{platform.system()} {platform.release()} ({platform.machine()})",
             "python": platform.python_version()}
    try:
        if platform.system() == "Darwin":
            specs["cpu"] = subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"], text=True).strip()
            specs["cores"] = subprocess.check_output(["sysctl", "-n", "hw.ncpu"], text=True).strip()
            mem = int(subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True))
        else:
            specs["cpu"] = platform.processor() or "unknown"
            specs["cores"] = str(os.cpu_count())
            mem = 0
        if mem:
            specs["memory"] = f"{mem / 2**30:.0f} GiB"
    except Exception:
        pass
    try:
        out = subprocess.check_output(["docker", "info", "--format", "{{.NCPU}} CPUs / {{.MemTotal}}"],
                                      text=True).strip()
        cpus, memb = out.split(" CPUs / ")
        specs["docker"] = f"{cpus} CPUs, {int(memb) / 2**30:.1f} GiB"
    except Exception:
        pass
    return specs


def write_report(path: Path, specs, config, tp, fr, quick: bool) -> bool:
    all_lat_p95 = max(r["p95"] for r in tp)
    best = max(tp, key=lambda r: r["sustained_per_s"])
    fr_vals = [r["freshness_s"] for r in fr if r["freshness_s"] is not None]
    detected = len(fr_vals)
    f_median = statistics.median(fr_vals) if fr_vals else float("nan")
    f_max = max(fr_vals) if fr_vals else float("nan")
    ok_volume = best["per_day"] >= TARGET_PER_DAY
    ok_latency = all_lat_p95 < TARGET_P95_S
    ok_fresh = detected == len(fr) and f_max <= TARGET_FRESHNESS_S
    ok_loss = all(r["loss"] == 0 for r in tp)
    mark = lambda ok: "✅ pass" if ok else "❌ fail"  # noqa: E731

    lines = [
        "# Benchmarks",
        "",
        f"Generated by `scripts/benchmark.py`{' (--quick)' if quick else ''} on "
        f"{time.strftime('%Y-%m-%d %H:%M %Z')}.",
        "",
        "## Summary",
        "",
        "| Metric | Target | Result | |",
        "|---|---|---|---|",
        f"| Ingestion volume | ≥ {TARGET_PER_DAY:,} posts/day | {best['per_day']:,} posts/day capacity "
        f"({best['sustained_per_s']:.0f} posts/s sustained) | {mark(ok_volume)} |",
        f"| Ingestion latency (p95, produce → enriched) | < {TARGET_P95_S * 1000:.0f} ms | "
        f"{all_lat_p95 * 1000:.0f} ms (worst rate) | {mark(ok_latency)} |",
        f"| Dashboard freshness (trend shift → WS client) | ≤ {TARGET_FRESHNESS_S:.0f} s | median "
        f"{f_median:.2f} s, max {f_max:.2f} s ({detected}/{len(fr)} detected) | {mark(ok_fresh)} |",
        f"| Message loss under load | 0 | {sum(r['loss'] for r in tp)} | {mark(ok_loss)} |",
        "",
        "## Throughput and latency",
        "",
        "| Offered rate | Sent | Enriched | Sustained | p50 | p95 | p99 | Max consumer lag | Daily capacity |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for r in tp:
        lines.append(
            f"| {r['rate']}/s | {r['sent']:,} | {r['enriched']:,} | {r['sustained_per_s']:.1f}/s | "
            f"{r['p50'] * 1000:.0f} ms | {r['p95'] * 1000:.0f} ms | {r['p99'] * 1000:.0f} ms | "
            f"{r['max_lag']} | {r['per_day']:,}/day |"
        )
    lines += [
        "",
        f"10,000 posts/day is ~0.12 posts/s; the slowest offered rate here is "
        f"{min(r['rate'] for r in tp)}/s (~{min(r['rate'] for r in tp) * 86_400:,}/day), "
        f"so headroom is ≥ {min(r['rate'] for r in tp) * 86_400 // TARGET_PER_DAY:,}×.",
        "",
        "## Freshness trials",
        "",
        f"Background {config['background_rate']} of synthetic posts; each trial injects a brand-new "
        f"phrase at {config['inject_rate']} and measures the time from the first injected post's "
        "`ingested_at` until a WebSocket client receives an update with that phrase in the top 10.",
        "",
        "| Trial | Phrase | Freshness | Rank when first seen |",
        "|---|---|---|---|",
    ]
    for r in fr:
        f = f"{r['freshness_s']:.2f} s" if r["freshness_s"] is not None else "not detected"
        lines.append(f"| {r['trial']} | `{r['phrase']}` | {f} | {r['rank'] or '—'} |")
    lines += ["", "## Configuration", ""]
    lines += [f"- **{k}:** {v}" for k, v in config.items()]
    lines += ["", "## Machine", ""]
    lines += [f"- **{k}:** {v}" for k, v in specs.items()]
    lines += [
        "",
        "## Method",
        "",
        "- Load generator, WebSocket client and pipeline all run in the same Docker network/VM, so",
        "  every timestamp comes from one clock (host↔VM clock drift on Docker Desktop would otherwise",
        "  contaminate cross-process latencies).",
        "- Load is generated by the synthetic producer (`acks=all`, idempotent, linger 5 ms);",
        "  every message is stamped with `ingested_at` immediately before send.",
        "- A separate group-less consumer reads `reddit.enriched` and records `processed_at − ingested_at`",
        "  for every post; *sustained* counts posts fully enriched inside the load window.",
        "- Consumer lag is sampled every 2 s from `GET /api/metrics` (sum over NLP workers).",
        "- Freshness is end-to-end: producer → Kafka → NLP worker → Kafka → trend engine (1 s emit) →",
        "  Kafka → API → WebSocket → client, all on one clock (same host).",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    return ok_volume and ok_latency and ok_fresh


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true", help="short phases (smoke test)")
    ap.add_argument("--out", type=Path,
                    default=Path(os.environ.get("BENCH_OUT", str(ROOT / "BENCHMARKS.md"))))
    args = ap.parse_args()
    s = get_settings()

    rates = [50, 100, 200]
    seconds = 15 if args.quick else 60
    warmup = 60 if args.quick else s.trend_window_s + 60
    trials = 3 if args.quick else 10

    health = api_json("/health")
    if health.get("status") != "ok":
        print(f"API not healthy: {health}", file=sys.stderr)
        return 2
    workers = api_json("/api/metrics")["summary"]["nlp_workers"]

    col = Collector()
    stop = asyncio.Event()
    collector = asyncio.create_task(collect_enriched(col, stop))
    await asyncio.sleep(2)
    producer = make_producer()
    await producer.start()
    sender = PostSender(producer, Metrics("benchmark"), _QuietLog())
    gen = SyntheticGenerator(seed=123)
    try:
        print("[1/2] throughput + latency", flush=True)
        tp = await throughput(sender, gen, col, rates, seconds, drain_s=5)
        print("[2/2] freshness", flush=True)
        fr = await freshness(sender, gen, trials, background_rate=20, inject_rate=20,
                             warmup_s=warmup, cooldown_s=10, timeout_s=20)
    finally:
        stop.set()
        await collector
        await producer.stop()

    config = {
        "nlp_workers": workers,
        "spaCy model": s.spacy_model,
        "NLP batch / flush": f"{s.nlp_batch_size} msgs / {s.nlp_flush_ms} ms",
        "trend window / baseline / emit": f"{s.trend_window_s} s / {s.trend_baseline_s} s / {s.trend_emit_interval_s} s",
        "partitions": "reddit.raw 6, reddit.enriched 6, trends.updates 1",
        "load phase length": f"{seconds} s per rate",
        "freshness warm-up": f"{warmup} s",
        "background_rate": "20 posts/s",
        "inject_rate": "20 posts/s",
    }
    ok = write_report(args.out, machine_specs(), config, tp, fr, args.quick)
    print(f"\nwrote {args.out}  ->  {'ALL TARGETS MET' if ok else 'SOME TARGETS MISSED'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
