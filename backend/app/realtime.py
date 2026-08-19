"""
Realtime hub for public train state only.
Never broadcasts raw GPS observations.
"""
from __future__ import annotations
import asyncio
import json
import threading
from typing import Any


class RealtimeHub:
    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue] = set()
        self._trip_subscribers: dict[str, set[asyncio.Queue]] = {}
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    async def subscribe(self, trip_id: str | None = None) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=64)
        with self._lock:
            if trip_id:
                self._trip_subscribers.setdefault(trip_id, set()).add(q)
            else:
                self._subscribers.add(q)
        return q

    async def unsubscribe(self, q: asyncio.Queue, trip_id: str | None = None) -> None:
        with self._lock:
            self._subscribers.discard(q)
            if trip_id and trip_id in self._trip_subscribers:
                self._trip_subscribers[trip_id].discard(q)
                if not self._trip_subscribers[trip_id]:
                    del self._trip_subscribers[trip_id]

    async def publish_public_state(self, payload: dict[str, Any]) -> None:
        message = json.dumps({"type": "train_state", "data": payload}, ensure_ascii=False)
        with self._lock:
            targets = set(self._subscribers)
            trip_id = payload.get("trip_id")
            if trip_id and trip_id in self._trip_subscribers:
                targets |= set(self._trip_subscribers[trip_id])
        for q in targets:
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                pass

    def publish_public_state_threadsafe(self, payload: dict[str, Any]) -> None:
        """Safe to call from sync FastAPI route threads."""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.publish_public_state(payload))
            return
        except RuntimeError:
            pass
        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self.publish_public_state(payload), self._loop)
            return
        # No active loop (e.g. TestClient without WS clients): no-op is fine
        # State is still available via GET /live


hub = RealtimeHub()
