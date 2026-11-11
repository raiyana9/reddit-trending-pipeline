"""Idempotently create the pipeline's Kafka topics (safe to run repeatedly)."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aiokafka.admin import AIOKafkaAdminClient, NewTopic  # noqa: E402
from aiokafka.errors import KafkaConnectionError  # noqa: E402

from shared.config import (  # noqa: E402
    TOPIC_DLQ,
    TOPIC_ENRICHED,
    TOPIC_RAW,
    TOPIC_TRENDS,
    get_settings,
)

RETENTION_MS = str(24 * 60 * 60 * 1000)

TOPICS: dict[str, int] = {
    TOPIC_RAW: 6,
    TOPIC_ENRICHED: 6,
    TOPIC_TRENDS: 1,
    TOPIC_DLQ: 1,
}


async def create_topics(retries: int = 30) -> list[str]:
    bootstrap = get_settings().kafka_bootstrap
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    for attempt in range(retries):
        try:
            await admin.start()
            break
        except KafkaConnectionError:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(1)
    try:
        existing = set(await admin.list_topics())
        missing = [
            NewTopic(
                name=name,
                num_partitions=parts,
                replication_factor=1,
                topic_configs={"retention.ms": RETENTION_MS},
            )
            for name, parts in TOPICS.items()
            if name not in existing
        ]
        if missing:
            await admin.create_topics(missing)
        for name in TOPICS:
            status = "created" if name in {t.name for t in missing} else "exists"
            print(f"{name:<18} partitions={TOPICS[name]}  {status}")
        return [t.name for t in missing]
    finally:
        await admin.close()


if __name__ == "__main__":
    asyncio.run(create_topics())
