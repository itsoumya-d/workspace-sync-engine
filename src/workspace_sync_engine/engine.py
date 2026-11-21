from __future__ import annotations

import asyncio
import logging
from typing import Any

from workspace_sync_engine.audit import DurableAuditLogger
from workspace_sync_engine.cache import MultiLevelCache
from workspace_sync_engine.cdc import ChangeDataCaptureStreamer
from workspace_sync_engine.db import (
    ConcurrencyConflictError,
    DatabaseSession,
)
from workspace_sync_engine.models import (
    AuditEvent,
    CDCEvent,
    ItemPriority,
    ItemStatus,
    Mutation,
    WorkItem,
)

logger = logging.getLogger("workspace_sync_engine")


class WorkspaceSyncEngine:
    """Core collaborative workspace engine orchestrating transactional state, CDC, cache, and audit."""

    def __init__(
        self,
        db: DatabaseSession | None = None,
        cache: MultiLevelCache | None = None,
        audit: DurableAuditLogger | None = None,
        cdc: ChangeDataCaptureStreamer | None = None,
    ) -> None:
        self.db = db or DatabaseSession()
        self.cache = cache or MultiLevelCache()
        self.audit = audit or DurableAuditLogger()
        self.cdc = cdc or ChangeDataCaptureStreamer(self.db)

    async def get_item(self, workspace_id: str, item_id: str) -> WorkItem | None:
        # Read-through cache
        cached = await self.cache.get_item(workspace_id, item_id)
        if cached:
            return cached

        # Database lookup
        item = await self.db.get_item(workspace_id, item_id)
        if item:
            await self.cache.put_item(workspace_id, item)
        return item

    async def list_items(
        self,
        workspace_id: str,
        status: ItemStatus | None = None,
        priority: ItemPriority | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[WorkItem], dict[str, Any]]:
        return await self.db.list_items(
            workspace_id=workspace_id,
            status=status,
            priority=priority,
            limit=limit,
            offset=offset,
        )

    async def apply_mutation(
        self,
        mutation: Mutation,
        max_retries: int = 3,
        backoff_base_ms: int = 15,
    ) -> tuple[WorkItem, CDCEvent, AuditEvent]:
        """Atomically applies a mutation with optimistic concurrency, outbox CDC, cache invalidation, and audit."""
        attempts = 0
        last_error: Exception | None = None

        while attempts < max_retries:
            attempts += 1
            try:
                # 1. Fetch previous state before lock
                prev_item = await self.db.get_item(mutation.workspace_id, mutation.item_id)

                # 2. Transactional execution
                async with self.db.transaction() as tx:
                    updated_item, cdc_event = await self.db.apply_mutation_atomic(tx, mutation)
                    # 3. Cryptographically linked audit record
                    audit_event = self.audit.record_mutation(mutation, prev_item, updated_item)

                # 4. Invalidate cache on commit
                await self.cache.handle_cdc_invalidation(cdc_event)

                # 5. Direct fan-out dispatch to real-time subscribers
                await self.cdc.emit_direct(cdc_event)

                return updated_item, cdc_event, audit_event

            except ConcurrencyConflictError as cce:
                last_error = cce
                if attempts < max_retries:
                    # Exponential jitter backoff
                    delay = (backoff_base_ms * (2 ** (attempts - 1))) / 1000.0
                    await asyncio.sleep(delay)
                    # Update expected version for retry if patch allowed
                    fresh = await self.db.get_item(mutation.workspace_id, mutation.item_id)
                    if fresh and mutation.expected_version is not None:
                        mutation.expected_version = fresh.version
                else:
                    raise

        raise last_error or ConcurrencyConflictError("Max retries exceeded on mutation")

    def get_audit_trail(self, workspace_id: str, item_id: str | None = None) -> list[AuditEvent]:
        return self.audit.get_audit_trail(workspace_id, item_id)

    def verify_audit_integrity(self, workspace_id: str) -> tuple[bool, int, str | None]:
        return self.audit.verify_audit_chain(workspace_id)

    def subscribe_cdc(self, workspace_id: str | None = None) -> asyncio.Queue[CDCEvent]:
        return self.cdc.subscribe(workspace_id)

    def unsubscribe_cdc(
        self, queue: asyncio.Queue[CDCEvent], workspace_id: str | None = None
    ) -> None:
        self.cdc.unsubscribe(queue, workspace_id)
