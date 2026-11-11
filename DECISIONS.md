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

## Phase 3
- **Score adds a velocity term.** `score = (z + z_velocity) * spread`, where
  `z = (count_window − expected)/sqrt(expected+1)` as specified and
  `z_velocity = max(velocity, 0)/sqrt(expected_30s + 1)`. Without it, a brand-new spike must
  out-count topics that have been accumulating in the 5-minute window for minutes, which makes
  the ≤5 s freshness target unreachable; with it, acceleration is rewarded immediately while
  steady topics are unaffected. Spread boost = +5% per extra distinct subreddit (capped at 6).
- **Baseline normalization uses observed history.** `expected = baseline_count / observed_s * window_s`
  where `observed_s = min(baseline_s, now − window_s − first_post)`; with < 60 s of baseline history,
  `expected = 0` (the engine reports raw window counts during warm-up).
- **Buckets keyed by `ingested_at`** (the pipeline's clock) rather than `created_utc`, so replays and
  live data behave the same and latency/freshness are measured on one clock.
- **State rebuild on restart:** the engine subscribes without a consumer group and seeks every
  partition of `reddit.enriched` to `now − (window + baseline)` via `offsets_for_times`, so a restart
  keeps its baseline. Single instance by design (`trends.updates` has 1 partition).
- **Near-duplicate merging:** among the top 200 candidates, phrases merge if their crude stem keys are
  equal (`laker`/`lakers`) or one is a contiguous word-subsequence of the other *and* they share a
  category. The highest-scoring member names the group; group count = max member count (members
  co-occur in the same posts, so summing would double-count).
- **Emit policy:** every `TREND_EMIT_INTERVAL_S`, if new posts arrived or a bucket boundary passed
  (scores age even without new input).
- **10× spike acceptance:** a real-time 10× spike of an *average* topic is only ~7 extra mentions in
  2 s, which is statistically indistinguishable from noise, so the test feeds 30 s of spike traffic and
  asserts the topic is #1 at each of the next 2 emit cycles. It also measures detection while the
  spike is in progress: 4 s in the unit test.
- **Memory:** unit test simulates 3 h at 5 posts/s with 30% never-repeating topics; tracked topics and
  buckets plateau once the 65-minute horizon fills.
- **Topic quality (NLP follow-up):** noun chunks must contain a NOUN/PROPN and must not be headed by a
  generic/opinion noun ("worst decision", "a disaster") unless they contain a proper noun.

## Phase 4
- **Frame envelope:** every WebSocket frame is `{"type": "snapshot"|"update"|"post"|"ping",
  "server_sent_at": <epoch s>, "data": {...}}`. A snapshot frame is always sent on connect (an empty
  structure if no trends exist yet) so clients have one code path.
- **Added `WS /ws/posts`** (optional in spec) for the dashboard's live ticker. It carries a trimmed post
  (no author hash, no snippet) and only deserializes enriched posts while at least one client is
  subscribed.
- **Slow clients:** bounded per-client queues (16 for trends, 64 for posts); on overflow the oldest
  frame is dropped, never blocking the Kafka consumer or other clients.
- **API Kafka consumers are group-less** (`group_id=None`, `latest`): every API replica must see every
  update, so a shared consumer group would be wrong.
- **`/api/metrics`** returns raw per-instance snapshots plus a `summary` (rates, totals, max-of-instances
  latency percentiles, lag, freshness, projected/day). The API's own `e2e_s` histogram is capped at
  300 samples so it reflects the last ~5 minutes rather than the engine's warm-up history.
- **Measured (local, replay at 60×):** newest contributing post → WebSocket send: median 0.18 s,
  max 0.36 s.

## Phase 5
- **Socket logic is a plain TS class** (`ReconnectingSocket`) with injectable timers/fetch/socket
  factory, wrapped by thin React hooks. That makes the reconnect schedule (0.5 → 1 → 2 → 4 → 8 → 10 s
  cap, reset on open) and polling fallback (`GET /api/trends` every 2 s only while disconnected)
  unit-testable with fake timers.
- **Rank animation** uses absolutely positioned rows keyed by topic with a CSS `top` transition, so
  React reuses DOM nodes and moves animate without an animation library.
- **Category filter is client-side**: the grid needs all 15 categories anyway, and switching filters is
  instant without reconnecting the socket (the server-side `?category=` filter is still available).
- **Header "Posts/s"** is pipeline throughput (enriched posts/s from `/api/metrics`); **Freshness** is
  computed client-side as `now − max_source_ingested_at` per the spec; **Ingest p95** is the worker's
  Kafka-produce → enriched latency.
- **Metrics fixes found while testing the UI:** histograms are now a 60 s rolling window (a past backlog
  had pinned p95 at minutes), and `/api/metrics` ignores per-instance snapshots older than 15 s (dead
  worker PIDs lingered in the Redis hash because live instances kept refreshing the key TTL); services
  also delete their own field on shutdown.
- **Topic display:** proper nouns keep their surface form ("united nations", not "united nation").
- Recharts is split into its own chunk (~155 kB gzip); `chunkSizeWarningLimit` raised to 600 kB.
