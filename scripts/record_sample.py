"""Record posts to JSONL for offline replay.

    python scripts/record_sample.py --source reddit -n 500 -o data/live_sample.jsonl
    python scripts/record_sample.py --source synthetic -n 500 -o data/sample_posts.jsonl

``--source synthetic`` builds a 60-minute timeline (~10k/day background rate) with a few
deliberate story bursts so a replay shows trends rising and falling.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.producer.synthetic import SyntheticGenerator  # noqa: E402
from shared.config import ROOT, get_settings  # noqa: E402

# (phrase, category, start offset s, duration s, count)
DEMO_STORIES = [
    ("GPT-6", "AI/ML", 600, 600, 45),
    ("the Switch 2", "Gaming", 1500, 500, 40),
    ("Bitcoin", "Finance/Crypto", 2300, 600, 45),
    ("the Champions League", "Sports", 3000, 400, 35),
]


def record_synthetic(n: int, out: Path, seed: int) -> int:
    gen = SyntheticGenerator(seed=seed)
    background = n - sum(s[4] for s in DEMO_STORIES)
    start = time.time() - 3600
    posts = gen.timeline(background, span_s=3600, start=start, stories=DEMO_STORIES)
    with out.open("w", encoding="utf-8") as fh:
        for p in posts:
            fh.write(p.model_dump_json(exclude={"schema_version", "ingested_at"}) + "\n")
    return len(posts)


async def record_reddit(n: int, out: Path) -> int:
    import asyncpraw

    from services.producer.reddit_producer import seed_subreddits, submission_to_raw

    s = get_settings()
    if not (s.reddit_client_id and s.reddit_client_secret):
        raise SystemExit("REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET not set")
    count = 0
    async with asyncpraw.Reddit(client_id=s.reddit_client_id, client_secret=s.reddit_client_secret,
                                user_agent=s.reddit_user_agent) as reddit:
        multi = await reddit.subreddit("+".join(seed_subreddits(s)))
        with out.open("w", encoding="utf-8") as fh:
            async for sub in multi.stream.submissions(skip_existing=False):
                post = submission_to_raw(sub)
                fh.write(post.model_dump_json(exclude={"schema_version", "ingested_at"}) + "\n")
                count += 1
                if count % 50 == 0:
                    print(f"recorded {count}/{n}")
                if count >= n:
                    break
    return count


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=["reddit", "synthetic"], default="reddit")
    p.add_argument("-n", type=int, default=500)
    p.add_argument("-o", "--out", type=Path, default=ROOT / "data" / "sample_posts.jsonl")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    if args.source == "synthetic":
        count = record_synthetic(args.n, args.out, args.seed)
    else:
        count = asyncio.run(record_reddit(args.n, args.out))
    print(f"wrote {count} posts -> {args.out}")


if __name__ == "__main__":
    main()
