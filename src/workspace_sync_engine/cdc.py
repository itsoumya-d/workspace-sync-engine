from __future__ import annotations

import asyncio
from typing import Callable, Coroutine, Dict, List, Optional, Set
from workspace_sync_engine.db import DatabaseSession
from workspace_sync_engine.models import CDCEvent


class ChangeDataCaptureStreamer:
    """Streams change events out of the transactional outbox to subscribers."""

    def __init__(self, db: DatabaseSession) -> None:
        self.db = db
        self._subscribers: Dict[str, Set[asyncio.Queue[CDCEvent]]] = {}
        self._global_subscribers: Set[asyncio.Queue[CDCEvent]] = set()
        self._last_processed_seq = 0
        self._running = False
        self._poll_task: Optional[asyncio.Task[None]] = None

    async def start(self, poll_interval_ms: int = 20) -> None:
        self._running = True
        self._poll_task = asyncio.create_task(self._poll_loop(poll_interval_ms))

    async def stop(self) -> None:
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass

    def subscribe(self, workspace_id: Optional[str] = None) -> asyncio.Queue[CDCEvent]:
        queue: asyncio.Queue[CDCEvent] = asyncio.Queue(maxsize=1000)
        if workspace_id:
            if workspace_id not in self._subscribers:
                self._subscribers[workspace_id] = set()
            self._subscribers[workspace_id].add(queue)
        else:
            self._global_subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[CDCEvent], workspace_id: Optional[str] = None) -> None:
        if workspace_id and workspace_id in self._subscribers:
            self._subscribers[workspace_id].discard(queue)
        self._global_subscribers.discard(queue)

    async def emit_direct(self, event: CDCEvent) -> None:
        """Immediately dispatch an event to active subscribers."""
        ws = event.workspace_id
        if ws in self._subscribers:
            for q in list(self._subscribers[ws]):
                try:
                    q.put_nowait(event)
                except asyncio.QueueFull:
                    pass  # Backpressure drop or buffer expansion
        for q in list(self._global_subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass

    async def _poll_loop(self, interval_ms: int) -> None:
        while self._running:
            events = await self.db.get_outbox_events(since_seq=self._last_processed_seq)
            for event in events:
                await self.emit_direct(event)
                self._last_processed_seq = max(self._last_processed_seq, event.sequence_num)
            await asyncio.sleep(interval_ms / 1000.0)
