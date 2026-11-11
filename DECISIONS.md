# Design decisions and deviations

Running log of choices that deviate from, or fill gaps in, the original spec.

## Phase 0
- **Python 3.12 via `uv`.** The host only had Python 3.9; `uv` provisions 3.12 into `.venv`.
- **Kafka image `apache/kafka:3.9.0`** (KRaft, single broker). Host listener `localhost:9092`,
  in-network listener `kafka:29092`. Auto topic creation is disabled; `scripts/create_topics.py`
  creates `reddit.raw` (6), `reddit.enriched` (6), `trends.updates` (1) and an extra
  **`reddit.dlq` (1)** for the NLP worker's dead letters (needed because auto-create is off).
- **Module name `shared/log.py`** instead of `shared/logging.py` to avoid shadowing the stdlib.
- **`TrendItem` extra fields:** `sample_urls` (parallel to `sample_titles`, so the dashboard can link
  titles to Reddit) and `subreddit_count` (spread). `TrendUpdate` adds `total_window`.
  `server_sent_at` is added by the API to each pushed WebSocket frame rather than stored in the schema.
- **Author hashing is enforced in the model.** A validator SHA-256s any `author` value that is not
  already a 64-char hex digest, so a raw username can never reach Kafka by accident.
- **Seed subreddits:** 70 across the 15 categories (spec suggested ~40–60). Reddit handles a
  multireddit this size fine; `REDDIT_SUBREDDITS` can override it.
- The category formerly "Entertainment (Film/TV)" is named **`Entertainment`** in the taxonomy to keep
  the identifier short; its keywords cover film and TV.

## Phase 1
- **Dedup uses per-key `SET seen:<id> 1 NX EX 172800`** rather than one Redis set: Redis sets can't
  expire individual members, so a per-key TTL is the only way to get a true 48h window per id.
- **`data/sample_posts.jsonl` is synthetic.** No Reddit credentials were available while building, so
  the 500-post sample was generated with `scripts/record_sample.py --source synthetic` (60-minute
  timeline at ~10k/day background rate plus four story bursts so replays show trends rising/falling).
  Replace it any time with `scripts/record_sample.py --source reddit -n 500`.
- **The live Reddit producer is unverified end-to-end** (no credentials). Its pure parts
  (submission conversion, author hashing, backoff, subreddit list) are unit-tested.
- Stream uses `pause_after=-1` so the loop wakes regularly and can honour SIGTERM promptly.
- Synthetic CLI: `--rate` is the background rate; the spike uses `--inject-trend/--at/--inject-rate/
  --inject-duration/--inject-category` (the spec's single `--rate 20/s` was ambiguous).
- Metrics transport chosen now for all services: JSON snapshots in Redis hash `metrics:<service>`
  (one field per instance) every 5s — see `shared/metrics.py`.

## Phase 2
- **aiokafka for the worker too** (not confluent-kafka): spaCy, not the client, is the bottleneck
  (~700 posts/s single-process with `en_core_web_sm`), and one client library keeps the code uniform.
  spaCy runs in `asyncio.to_thread` so consumer heartbeats and producer I/O keep flowing.
- **Pipeline components:** everything in `en_core_web_sm` is kept (tagger/attribute_ruler feed the
  lemmatizer, parser gives noun chunks, NER gives entities); `vader_sentiment` is a registered
  `@Language.component` appended last.
- **Topic normalization:** noun chunks are lemmatized; entity phrases keep their (lowercased) surface
  form so names aren't mangled ("kansas city chiefs" not "kansas city chief"). A small lemma override
  table fixes spaCy oddities ("datum" -> "data"). Phrases > 5 tokens, URLs and `r/…`/`u/…` refs are dropped.
- **Keyword matching is lemma-aware** via two `PhraseMatcher` passes: LOWER on the doc, and ORTH on a
  lemma-lowercased copy of the doc (PhraseMatcher's own `LEMMA` attr is case-sensitive for proper nouns).
- **Classifier weights:** subreddit prior 5.0, keyword match 1.0 (1.5 in title, capped at 6 per
  category), entity hint 0.3; softmax for confidence. With no evidence at all the post falls back to
  `Lifestyle` (general-interest catch-all) with ~1/15 confidence.
- **Measured (laptop, 1 worker, 50 posts/s):** 500/500 enriched; ingest latency p50 99 ms, p95 162 ms.
  p50 ≈ the 100 ms flush window, as expected at this rate.
