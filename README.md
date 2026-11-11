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
