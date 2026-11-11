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
