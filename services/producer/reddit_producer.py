"""Live Reddit producer: asyncpraw submission stream -> ``reddit.raw``.

- OAuth "script" app credentials from env (REDDIT_CLIENT_ID / _SECRET / _USER_AGENT)
- Streams new submissions from the multireddit of all seed subreddits
- asyncpraw honours Reddit's rate-limit headers; on 429 / 5xx / network errors we restart the
  stream with exponential backoff + full jitter. A single bad post never stops the loop.
- Dedup via Redis ``SET seen:<id> 1 NX EX 172800`` (48h)
"""

from __future__ import annotations

import asyncio
import random
from typing import Any

import redis.asyncio as aioredis

from services.producer.common import PostSender
from services.producer.synthetic import load_taxonomy
from shared.config import REDIS_SEEN_KEY_PREFIX, Settings
from shared.schemas import RawPost

SEEN_TTL_S = 48 * 3600
BACKOFF_BASE_S = 1.0
BACKOFF_MAX_S = 120.0


def seed_subreddits(settings: Settings) -> list[str]:
    if settings.reddit_subreddits.strip():
        return [s.strip() for s in settings.reddit_subreddits.split(",") if s.strip()]
    subs: list[str] = []
    for cat in load_taxonomy().values():
        subs.extend(cat["subreddits"])
    return sorted(set(subs), key=str.lower)


def backoff_delay(attempt: int, base: float = BACKOFF_BASE_S, cap: float = BACKOFF_MAX_S) -> float:
    """Exponential backoff with full jitter."""
    return random.uniform(0, min(cap, base * 2**attempt))


def submission_to_raw(sub: Any) -> RawPost:
    author = getattr(sub, "author", None)
    return RawPost(
        id=sub.id,
        subreddit=str(sub.subreddit.display_name),
        title=sub.title or "",
        selftext=getattr(sub, "selftext", "") or "",
        author=str(author) if author is not None else "[deleted]",  # hashed by the model
        score=int(getattr(sub, "score", 0) or 0),
        num_comments=int(getattr(sub, "num_comments", 0) or 0),
        url=f"https://www.reddit.com{sub.permalink}",
        created_utc=float(sub.created_utc),
        ingested_at=0.0,  # stamped by PostSender right before send
        source="reddit",
    )


async def is_new(redis: aioredis.Redis, post_id: str) -> bool:
    return bool(await redis.set(f"{REDIS_SEEN_KEY_PREFIX}{post_id}", 1, nx=True, ex=SEEN_TTL_S))


async def run_reddit(
    sender: PostSender, settings: Settings, redis: aioredis.Redis, stop: asyncio.Event,
    limit: int | None = None,
) -> None:
    import asyncpraw
    import asyncprawcore

    if not (settings.reddit_client_id and settings.reddit_client_secret):
        raise SystemExit("REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET are not set (see .env.example)")

    log, metrics = sender.log, sender.metrics
    subs = seed_subreddits(settings)
    log.info("starting reddit stream", extra={"subreddits": len(subs)})
    retryable = (
        asyncprawcore.exceptions.TooManyRequests,
        asyncprawcore.exceptions.ServerError,
        asyncprawcore.exceptions.RequestException,
        asyncprawcore.exceptions.ResponseException,
        asyncio.TimeoutError,
    )
    attempt = 0
    produced = 0
    async with asyncpraw.Reddit(
        client_id=settings.reddit_client_id,
        client_secret=settings.reddit_client_secret,
        user_agent=settings.reddit_user_agent,
    ) as reddit:
        reddit.read_only = True
        multi = await reddit.subreddit("+".join(subs))
        while not stop.is_set():
            try:
                async for submission in multi.stream.submissions(skip_existing=True, pause_after=-1):
                    if stop.is_set():
                        break
                    if submission is None:  # stream paused with no new items
                        continue
                    attempt = 0
                    try:
                        if not await is_new(redis, submission.id):
                            metrics.inc("duplicates_skipped")
                            continue
                        await sender.send(submission_to_raw(submission))
                        produced += 1
                        if limit and produced >= limit:
                            stop.set()
                            break
                    except Exception as exc:  # one bad post must not kill the stream
                        metrics.inc("bad_posts")
                        log.warning("skipping bad post", extra={"post_id": getattr(submission, "id", None),
                                                                "error": repr(exc)})
            except retryable as exc:
                delay = backoff_delay(attempt)
                attempt += 1
                metrics.inc("stream_restarts")
                log.warning("reddit stream error, backing off",
                            extra={"error": repr(exc), "attempt": attempt, "sleep_s": round(delay, 2)})
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except TimeoutError:
                    pass
