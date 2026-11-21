from __future__ import annotations

import datetime as dt
from typing import Any

from workspace_sync_engine.models import CDCEvent, WorkItem


class MultiLevelCache:
    """L1 Memory + L2 Distributed Cache with event-driven invalidation."""

    def __init__(self, default_ttl_sec: int = 300) -> None:
        self.default_ttl = default_ttl_sec
        self._l1_items: dict[str, tuple[WorkItem, dt.datetime]] = {}  # key -> (item, expiry)
        self._l2_store: dict[str, tuple[str, dt.datetime]] = {}  # simulated Redis KV
        self._hits = 0
        self._misses = 0
        self._invalidations = 0

    def _item_key(self, workspace_id: str, item_id: str) -> str:
        return f"ws:{workspace_id}:item:{item_id}"

    def _collection_prefix(self, workspace_id: str) -> str:
        return f"ws:{workspace_id}:list:"

    async def get_item(self, workspace_id: str, item_id: str) -> WorkItem | None:
        key = self._item_key(workspace_id, item_id)
        now = dt.datetime.now(dt.UTC)

        # Check L1
        if key in self._l1_items:
            item, exp = self._l1_items[key]
            if exp > now:
                self._hits += 1
                return item
            else:
                del self._l1_items[key]

        # Check L2
        if key in self._l2_store:
            raw_json, exp = self._l2_store[key]
            if exp > now:
                item = WorkItem.model_validate_json(raw_json)
                self._l1_items[key] = (item, exp)
                self._hits += 1
                return item
            else:
                del self._l2_store[key]

        self._misses += 1
        return None

    async def put_item(self, workspace_id: str, item: WorkItem) -> None:
        key = self._item_key(workspace_id, item.id)
        exp = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=self.default_ttl)
        self._l1_items[key] = (item, exp)
        self._l2_store[key] = (item.model_dump_json(), exp)

    async def handle_cdc_invalidation(self, event: CDCEvent) -> None:
        """Invalidates L1 and L2 cache entries when CDC events arrive."""
        key = self._item_key(event.workspace_id, event.entity_id)
        if key in self._l1_items:
            del self._l1_items[key]
            self._invalidations += 1
        if key in self._l2_store:
            del self._l2_store[key]
            self._invalidations += 1

        # Also purge any collection queries for this workspace
        prefix = self._collection_prefix(event.workspace_id)
        l1_purge = [k for k in self._l1_items if k.startswith(prefix)]
        for k in l1_purge:
            del self._l1_items[k]
            self._invalidations += 1
        l2_purge = [k for k in self._l2_store if k.startswith(prefix)]
        for k in l2_purge:
            del self._l2_store[k]
            self._invalidations += 1

    @property
    def metrics(self) -> dict[str, Any]:
        total = self._hits + self._misses
        hit_ratio = (self._hits / total) if total > 0 else 0.0
        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_ratio": round(hit_ratio, 3),
            "invalidations": self._invalidations,
            "l1_size": len(self._l1_items),
            "l2_size": len(self._l2_store),
        }
