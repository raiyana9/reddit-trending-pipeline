# Real-Time Reddit Trending Pipeline

Streaming system that ingests Reddit posts through **Kafka**, enriches them with **spaCy** NLP
(keyphrases, entities, 15-way category, VADER sentiment), detects trending topics over sliding
windows, and pushes live updates to a **React** dashboard over **FastAPI WebSockets**.

## Quick start

Requirements: Docker, [uv](https://docs.astral.sh/uv/), Node 20+.

```bash
uv sync                       # Python 3.12 venv + deps (incl. en_core_web_sm)
make up                       # Kafka (KRaft) + Redis, creates topics
uv run pytest -m "not integration"
```

## Infrastructure (Phase 0)

| Component | Address |
|---|---|
| Kafka (host clients) | `localhost:9092` |
| Kafka (in compose network) | `kafka:29092` |
| Redis | `localhost:6379` |

Topics (24h retention): `reddit.raw` (6 partitions), `reddit.enriched` (6), `trends.updates` (1),
`reddit.dlq` (1). `python scripts/create_topics.py` is idempotent.

All configuration is via environment variables — see [.env.example](.env.example).
Design deviations are logged in [DECISIONS.md](DECISIONS.md).

## Producers (Phase 1)

```bash
uv run python -m services.producer replay --speed 10          # data/*.jsonl at 10x
uv run python -m services.producer replay --rate 50 --loop     # fixed rate, forever
uv run python -m services.producer synthetic --rate 20 --duration 2m \
    --inject-trend "quantum toaster" --at 30s --inject-rate 20/s
uv run python -m services.producer reddit                      # live; needs REDDIT_* in .env
uv run python scripts/record_sample.py --source reddit -n 500  # capture live posts to JSONL
```

Every message is keyed by post id and stamped with `ingested_at` immediately before send.
Authors are SHA-256 hashed. The live producer dedups through Redis (`seen:<id>`, 48h TTL).

## NLP worker (Phase 2)

```bash
uv run python -m services.nlp_worker                 # consumer group "nlp-workers"
docker compose --profile app up -d --scale nlp_worker=3
```

spaCy `en_core_web_sm` (+ custom `vader_sentiment` component) extracts up to 8 normalized
keyphrases, filtered entities, a category (15-way: subreddit prior + lemma-aware `PhraseMatcher`
keywords + entity hints, softmax confidence) and a VADER compound sentiment. Micro-batches flush at
`NLP_BATCH_SIZE` messages or `NLP_FLUSH_MS` ms; offsets commit after the enriched batch is acked.
Failures go to `reddit.dlq`. Swap models with `SPACY_MODEL=en_core_web_md`.

## Trend engine (Phase 3)

```bash
uv run python -m services.trend_engine
```

10 s buckets per topic; 5-min window vs 60-min baseline (normalized), score =
`(z + z_velocity) × spread` with a ≥3-mention gate, near-duplicate merging, and a `TrendUpdate`
(top 50 + all 15 category stats) every second to `trends.updates` and Redis `trends:latest`.
On restart it rebuilds state from the last 65 minutes of `reddit.enriched`.

## API (Phase 4)

```bash
uv run python -m services.api                       # http://localhost:8000/docs
curl localhost:8000/api/trends?category=Gaming
websocat ws://localhost:8000/ws/trends              # snapshot, then 1 update/s
```

| Endpoint | Description |
|---|---|
| `GET /health` | Kafka + Redis reachability (503 if degraded) |
| `GET /api/trends` | Current snapshot from Redis (`?category=` filter) |
| `GET /api/categories` | The 15 categories with window volume + avg sentiment |
| `GET /api/metrics` | Aggregated live pipeline metrics |
| `WS /ws/trends` | Snapshot on connect, then every `TrendUpdate` (`?category=` filter) |
| `WS /ws/posts` | Stream of newly enriched posts (ticker) |

Frames: `{"type", "server_sent_at", "data"}`; `{"type": "ping"}` after 15 s of silence.

## Dashboard (Phase 5)

```bash
cd frontend && npm install
npm run dev        # http://localhost:5173 (expects the API on :8000)
npm test -- --run  # Vitest: reconnect hook, trend diffing, components
npm run build      # type-check + production build
```

Header (connection dot, posts/s, freshness, ingest p95), top-20 trending list with animated rank
changes, sparklines, sentiment chips and category tags; 15-tile category grid (click to filter);
topic drawer with count/sentiment history and sample posts; live post ticker (`/ws/posts`).
WebSocket auto-reconnects (exponential backoff, max 10 s) and polls `/api/trends` while disconnected.
Dark mode follows the OS and can be toggled.
