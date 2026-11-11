"""Replay and synthetic producer loops."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

from services.producer.common import PostSender, read_jsonl
from services.producer.synthetic import SyntheticGenerator
from shared.schemas import RawPost


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    if seconds <= 0:
        return
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass


def load_posts(paths: list[Path]) -> list[RawPost]:
    posts = []
    for rec in read_jsonl(paths):
        rec["source"] = "replay"
        rec.setdefault("ingested_at", 0.0)
        posts.append(RawPost.model_validate(rec))
    posts.sort(key=lambda p: p.created_utc)
    return posts


async def run_replay(
    sender: PostSender, paths: list[Path], stop: asyncio.Event,
    speed: float | None = None, rate: float | None = None, loop: bool = False,
) -> int:
    """Replay JSONL posts either at ``speed``× their original spacing or at a fixed ``rate``/s.

    ``ingested_at`` is rewritten to now. When looping, ids get a ``-<n>`` suffix so each pass
    is a distinct post.
    """
    posts = load_posts(paths)
    if not posts:
        raise SystemExit(f"no posts found in {', '.join(map(str, paths))}")
    sent = 0
    pass_no = 0
    while not stop.is_set():
        t0 = time.monotonic()
        first_ts = posts[0].created_utc
        for i, post in enumerate(posts):
            if stop.is_set():
                break
            if rate:
                target = t0 + i / rate
            else:
                target = t0 + (post.created_utc - first_ts) / (speed or 1.0)
            await _sleep_or_stop(stop, target - time.monotonic())
            out = post.model_copy()
            if pass_no:
                out.id = f"{post.id}-{pass_no}"
            out.created_utc = time.time()
            await sender.send(out)
            sent += 1
        pass_no += 1
        if not loop:
            break
    await sender.producer.flush()
    sender.log.info("replay finished", extra={"sent": sent, "passes": pass_no})
    return sent


async def run_synthetic(
    sender: PostSender, stop: asyncio.Event, rate: float, duration_s: float | None = None,
    inject_phrase: str | None = None, inject_at_s: float = 30.0, inject_rate: float = 20.0,
    inject_duration_s: float = 30.0, inject_category: str = "Technology", seed: int | None = None,
) -> int:
    """Background posts at ``rate``/s; optionally a spike of ``inject_phrase`` at ``inject_at_s``."""
    gen = SyntheticGenerator(seed=seed)
    t0 = time.monotonic()
    bg_sent = inj_sent = 0
    while not stop.is_set():
        elapsed = time.monotonic() - t0
        if duration_s is not None and elapsed >= duration_s:
            break
        while bg_sent < rate * elapsed:
            await sender.send(gen.make_post())
            bg_sent += 1
        if inject_phrase and inject_at_s <= elapsed < inject_at_s + inject_duration_s:
            due = inject_rate * (elapsed - inject_at_s)
            while inj_sent < due:
                await sender.send(gen.make_injected(inject_phrase, inject_category))
                inj_sent += 1
        await _sleep_or_stop(stop, 0.005)
    await sender.producer.flush()
    sender.log.info("synthetic finished", extra={"background": bg_sent, "injected": inj_sent})
    return bg_sent + inj_sent
