"""Producer CLI.

    python -m services.producer reddit                      # live Reddit (needs creds)
    python -m services.producer replay --speed 10           # data/*.jsonl at 10x real time
    python -m services.producer replay --rate 50 --loop     # fixed 50 posts/s, forever
    python -m services.producer synthetic --rate 20 --inject-trend "quantum toaster" \
        --at 30s --inject-rate 20/s --duration 2m
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

import redis.asyncio as aioredis

from services.producer.common import PostSender, parse_duration, parse_rate
from shared.config import ROOT, get_settings
from shared.kafka import install_shutdown_handler, make_producer
from shared.log import get_logger
from shared.metrics import Metrics


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="services.producer")
    sub = p.add_subparsers(dest="mode", required=True)

    r = sub.add_parser("reddit", help="stream live submissions")
    r.add_argument("--limit", type=int, default=None, help="stop after N posts")

    rp = sub.add_parser("replay", help="replay JSONL files")
    rp.add_argument("files", nargs="*", type=Path, help="default: data/*.jsonl")
    g = rp.add_mutually_exclusive_group()
    g.add_argument("--speed", type=float, default=None, help="speedup vs original spacing")
    g.add_argument("--rate", type=parse_rate, default=None, help="fixed posts/sec")
    rp.add_argument("--loop", action="store_true", help="repeat forever")

    s = sub.add_parser("synthetic", help="generate fake posts")
    s.add_argument("--rate", type=parse_rate, default=7 / 60, help="background posts/sec (e.g. 50 or 50/s)")
    s.add_argument("--duration", type=parse_duration, default=None, help="e.g. 60s, 5m")
    s.add_argument("--inject-trend", dest="inject_phrase", default=None)
    s.add_argument("--at", dest="inject_at", type=parse_duration, default=30.0)
    s.add_argument("--inject-rate", type=parse_rate, default=20.0)
    s.add_argument("--inject-duration", type=parse_duration, default=30.0)
    s.add_argument("--inject-category", default="Technology")
    s.add_argument("--seed", type=int, default=None)
    return p


async def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    log = get_logger(f"producer.{args.mode}")
    metrics = Metrics(f"producer_{args.mode}")
    stop = asyncio.Event()
    install_shutdown_handler(stop)

    redis = aioredis.from_url(settings.redis_url)
    producer = make_producer()
    await producer.start()
    sender = PostSender(producer, metrics, log)
    bg = [
        asyncio.create_task(sender.log_throughput_forever(stop)),
        asyncio.create_task(metrics.publish_forever(redis, settings.metrics_interval_s, stop)),
    ]
    try:
        if args.mode == "reddit":
            from services.producer.reddit_producer import run_reddit

            await run_reddit(sender, settings, redis, stop, limit=args.limit)
        elif args.mode == "replay":
            from services.producer.replay import run_replay

            files = args.files or sorted((ROOT / "data").glob("*.jsonl"))
            speed = args.speed if (args.speed or args.rate) else 1.0
            await run_replay(sender, files, stop, speed=speed, rate=args.rate, loop=args.loop)
        else:
            from services.producer.replay import run_synthetic

            await run_synthetic(
                sender, stop, rate=args.rate, duration_s=args.duration,
                inject_phrase=args.inject_phrase, inject_at_s=args.inject_at,
                inject_rate=args.inject_rate, inject_duration_s=args.inject_duration,
                inject_category=args.inject_category, seed=args.seed,
            )
    finally:
        stop.set()
        await producer.flush()
        await producer.stop()
        await asyncio.gather(*bg, return_exceptions=True)
        await redis.aclose()
        log.info("producer stopped", extra={"total": int(metrics.counters["posts_produced"])})


if __name__ == "__main__":
    asyncio.run(main())
