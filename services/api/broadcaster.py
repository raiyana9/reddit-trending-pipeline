"""In-process fan-out: one Kafka consumer -> many WebSocket clients.

Each subscriber owns a bounded ``asyncio.Queue``. Publishing never blocks: if a slow client's queue
is full, its oldest message is dropped to make room (the newest trend state is what matters).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any


@dataclass(eq=False)
class Subscriber:
    queue: asyncio.Queue[dict[str, Any]]
    category: str | None = None
    dropped: int = 0


@dataclass
class Broadcaster:
    max_queue: int = 16
    subscribers: set[Subscriber] = field(default_factory=set)
    published: int = 0
    dropped: int = 0

    def subscribe(self, category: str | None = None) -> Subscriber:
        sub = Subscriber(queue=asyncio.Queue(maxsize=self.max_queue), category=category)
        self.subscribers.add(sub)
        return sub

    def unsubscribe(self, sub: Subscriber) -> None:
        self.subscribers.discard(sub)

    def publish(self, message: dict[str, Any]) -> None:
        self.published += 1
        for sub in list(self.subscribers):
            if sub.queue.full():
                try:
                    sub.queue.get_nowait()
                except asyncio.QueueEmpty:  # pragma: no cover - race-free in a single loop
                    pass
                sub.dropped += 1
                self.dropped += 1
            sub.queue.put_nowait(message)

    def __len__(self) -> int:
        return len(self.subscribers)
