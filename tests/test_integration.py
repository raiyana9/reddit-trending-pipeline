"""Integration tests against real Kafka + Redis (``make up``). Run with ``pytest -m integration``.

The NLP worker runs in-process under a unique consumer group, so it never steals partitions from
other workers that may be running.
"""

import asyncio
import socket
import time
import uuid

import pytest
from aiokafka import AIOKafkaConsumer

from services.nlp_worker.worker import NlpWorker
from services.producer.synthetic import SyntheticGenerator
from shared.config import TOPIC_DLQ, TOPIC_ENRICHED, TOPIC_RAW, get_settings
from shared.kafka import make_producer
from shared.schemas import DeadLetter, EnrichedPost

pytestmark = pytest.mark.integration


def _kafka_up() -> bool:
    host, port = get_settings().kafka_bootstrap.split(",")[0].split(":")
    try:
        with socket.create_connection((host, int(port)), timeout=1):
            return True
    except OSError:
        return False


@pytest.fixture(autouse=True)
def require_kafka():
    if not _kafka_up():
        pytest.skip("Kafka not reachable (run `make up`)")


async def _listen(topic: str) -> AIOKafkaConsumer:
    c = AIOKafkaConsumer(topic, bootstrap_servers=get_settings().kafka_bootstrap, group_id=None,
                         auto_offset_reset="latest")
    await c.start()
    for _ in range(100):  # wait until partitions are assigned and positioned at the end
        if c.assignment():
            for tp in c.assignment():
                await c.position(tp)
            break
        await asyncio.sleep(0.05)
    return c


@pytest.mark.timeout(90)
async def test_worker_enriches_posts_and_dead_letters_garbage():
    run = uuid.uuid4().hex[:8]
    worker = NlpWorker(get_settings(), group_id=f"it-{run}", offset_reset="latest")
    task = asyncio.create_task(worker.run())
    for _ in range(300):
        if worker.consumer is not None and worker.consumer.assignment():
            break
        await asyncio.sleep(0.1)
    assert worker.consumer is not None and worker.consumer.assignment()
    for tp in worker.consumer.assignment():
        await worker.consumer.position(tp)

    enriched_c = await _listen(TOPIC_ENRICHED)
    dlq_c = await _listen(TOPIC_DLQ)
    producer = make_producer()
    await producer.start()
    gen = SyntheticGenerator(seed=99)
    ids = set()
    try:
        for i in range(20):
            post = gen.make_post()
            post.id = f"it{run}{i}"
            post.ingested_at = time.time()
            ids.add(post.id)
            await producer.send(TOPIC_RAW, key=post.id.encode(), value=post.to_bytes())
        bad_key = f"bad{run}".encode()
        await producer.send(TOPIC_RAW, key=bad_key, value=b"{not json")
        await producer.flush()

        seen: dict[str, EnrichedPost] = {}
        dead: list[DeadLetter] = []
        deadline = time.time() + 45
        while time.time() < deadline and (len(seen) < len(ids) or not dead):
            for recs in (await enriched_c.getmany(timeout_ms=200)).values():
                for r in recs:
                    e = EnrichedPost.from_bytes(r.value)
                    if e.id in ids:
                        seen[e.id] = e
            for recs in (await dlq_c.getmany(timeout_ms=50)).values():
                for r in recs:
                    if r.key == bad_key:
                        dead.append(DeadLetter.from_bytes(r.value))

        assert set(seen) == ids
        assert all(e.processed_at - e.ingested_at < 5 for e in seen.values())
        assert all(e.category and -1 <= e.sentiment <= 1 for e in seen.values())
        assert dead and "decode" in dead[0].error and dead[0].source_topic == TOPIC_RAW
    finally:
        worker.stop.set()
        await asyncio.wait_for(task, timeout=20)
        await producer.stop()
        await enriched_c.stop()
        await dlq_c.stop()
