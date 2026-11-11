"""Thin aiokafka factories so every service uses the same, correct settings."""

from __future__ import annotations

import asyncio
import signal
from collections.abc import Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from shared.config import get_settings


def make_producer(**overrides: object) -> AIOKafkaProducer:
    """Idempotent, acks=all producer with a small linger for batching."""
    s = get_settings()
    kwargs: dict[str, object] = {
        "bootstrap_servers": s.kafka_bootstrap,
        "acks": "all",
        "enable_idempotence": True,
        "linger_ms": 5,
        "compression_type": None,
        "max_batch_size": 256 * 1024,
        "request_timeout_ms": 30_000,
    }
    kwargs.update(overrides)
    return AIOKafkaProducer(**kwargs)  # type: ignore[arg-type]


def make_consumer(*topics: str, group_id: str | None, **overrides: object) -> AIOKafkaConsumer:
    s = get_settings()
    kwargs: dict[str, object] = {
        "bootstrap_servers": s.kafka_bootstrap,
        "group_id": group_id,
        "enable_auto_commit": False,
        "auto_offset_reset": "latest",
        "fetch_max_wait_ms": 50,
        "max_poll_records": 500,
        "session_timeout_ms": 10_000,
        "heartbeat_interval_ms": 3_000,
    }
    kwargs.update(overrides)
    return AIOKafkaConsumer(*topics, **kwargs)  # type: ignore[arg-type]


def install_shutdown_handler(stop: asyncio.Event, on_signal: Callable[[], None] | None = None) -> None:
    """Set ``stop`` on SIGTERM/SIGINT so loops can flush and commit before exiting."""
    loop = asyncio.get_running_loop()

    def _handle() -> None:
        stop.set()
        if on_signal:
            on_signal()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle)
        except (NotImplementedError, RuntimeError):  # pragma: no cover - non-unix
            pass
