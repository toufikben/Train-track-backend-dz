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
    """Pub/sub hub for aggregated train state.

    Subscribers receive JSON strings. Sync FastAPI routes call the
    ``*_threadsafe`` publishers: dispatch goes to the app's bound loop when
    the caller is on a different thread, and is scheduled directly when the
    caller is already on the bound loop (the only unsafe case — a self-loop
    — is routed to a dedicated fallback daemon loop that executes publishes
    without blocking the caller).
    """

    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue] = set()
        self._trip_subscribers: dict[str, set[asyncio.Queue]] = {}
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._fallback_loop: _FallbackLoop | None = None

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

    async def unsubscribe(self, q: asyncio.Queue,
                          trip_id: str | None = None) -> None:
        with self._lock:
            self._subscribers.discard(q)
            if trip_id and trip_id in self._trip_subscribers:
                self._trip_subscribers[trip_id].discard(q)
                if not self._trip_subscribers[trip_id]:
                    del self._trip_subscribers[trip_id]

    async def publish_public_state(self, payload: dict[str, Any]) -> None:
        message = json.dumps({"type": "train_state", "data": payload},
                             ensure_ascii=False)
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

    async def publish_train_gone(self, train_id: str,
                                 trip_id: str | None = None) -> None:
        """Broadcast that a train left public view (expired / UNKNOWN).

        Clients remove the marker immediately instead of waiting for the next
        poll cycle. Sent to ALL subscribers — clients key markers by id.
        """
        message = json.dumps({"type": "train_gone",
                              "data": {"id": train_id, "trip_id": trip_id}},
                             ensure_ascii=False)
        with self._lock:
            targets = set(self._subscribers)
            if trip_id and trip_id in self._trip_subscribers:
                targets |= set(self._trip_subscribers[trip_id])
        for q in targets:
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                pass

    def _dispatch_threadsafe(self, coro) -> None:
        if not (self._loop and self._loop.is_running()):
            self._fallback().publish(coro)
            return
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            # Caller thread has no running loop (typical FastAPI worker
            # thread): dispatch to the bound app loop.
            asyncio.run_coroutine_threadsafe(coro, self._loop)
            return
        if running is self._loop:
            # Caller is on the bound loop itself — scheduling there would
            # self-deadlock (nothing drains the loop until the caller
            # returns). Use the fallback daemon loop instead.
            self._fallback().publish(coro)
            return
        # Caller is on a DIFFERENT running loop: the caller's loop keeps
        # running independently, so run_coroutine_threadsafe executes when
        # the bound loop iterates (the normal production path under uvicorn
        # when a sync route runs in a worker thread of a different event
        # loop setup, or an async test loop).
        asyncio.run_coroutine_threadsafe(coro, self._loop)

    def _fallback(self) -> _FallbackLoop:
        if self._fallback_loop is None or not self._fallback_loop.is_alive():
            with self._lock:
                if self._fallback_loop is None \
                        or not self._fallback_loop.is_alive():
                    self._fallback_loop = _FallbackLoop()
                    self._fallback_loop.start()
        return self._fallback_loop

    def publish_public_state_threadsafe(self, payload: dict[str, Any]) -> None:
        self._dispatch_threadsafe(self.publish_public_state(payload))

    def publish_train_gone_threadsafe(self, train_id: str,
                                      trip_id: str | None = None) -> None:
        self._dispatch_threadsafe(
            self.publish_train_gone(train_id, trip_id))


class _FallbackLoop(threading.Thread):
    """Daemon event loop executing publish coroutines when the app's bound
    loop cannot be used safely (caller on the bound loop itself, or no loop
    available)."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self._queue: asyncio.Queue = asyncio.Queue()

    def run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def worker() -> None:
            while True:
                coro = await self._queue.get()
                try:
                    await coro
                except Exception:  # noqa: BLE001
                    pass

        loop.create_task(worker())
        loop.run_forever()

    def publish(self, coro) -> None:
        self._queue.put_nowait(coro)


hub = RealtimeHub()
