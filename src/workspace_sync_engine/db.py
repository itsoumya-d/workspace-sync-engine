from __future__ import annotations

import asyncio
import copy
import datetime as dt
from typing import Any

from workspace_sync_engine.models import (
    CDCEvent,
    ItemPriority,
    ItemStatus,
    Mutation,
    MutationType,
    WorkItem,
)


class ConcurrencyConflictError(Exception):
    """Raised when expected version does not match current state version."""


class NotFoundError(Exception):
    """Raised when an entity is not found."""


class TransactionContext:
    def __init__(self, db: DatabaseSession) -> None:
        self.db = db
        self.snapshot_items: dict[str, dict[str, WorkItem]] = {}
        self.pending_outbox: list[CDCEvent] = []

    async def __aenter__(self) -> TransactionContext:
        await self.db.acquire_lock()
        # Deep-copy current state for rollback capability
        self.snapshot_items = {
            ws: {k: copy.deepcopy(v) for k, v in items.items()}
            for ws, items in self.db._items.items()
        }
        self.pending_outbox = []
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is not None:
            # Rollback
            self.db._items = self.snapshot_items
            self.pending_outbox.clear()
        else:
            # Commit outbox events
            for event in self.pending_outbox:
                self.db._outbox.append(event)
                self.db._sequence_counter += 1
        self.db.release_lock()


class DatabaseSession:
    def __init__(self, max_connections: int = 20) -> None:
        self.max_connections = max_connections
        self._lock = asyncio.Lock()
        self._items: dict[str, dict[str, WorkItem]] = {}  # workspace_id -> {item_id -> WorkItem}
        self._outbox: list[CDCEvent] = []
        self._sequence_counter = 1
        self._query_plan_log: list[dict[str, Any]] = []

    async def acquire_lock(self) -> None:
        await self._lock.acquire()

    def release_lock(self) -> None:
        self._lock.release()

    def transaction(self) -> TransactionContext:
        return TransactionContext(self)

    async def get_item(self, workspace_id: str, item_id: str) -> WorkItem | None:
        ws = self._items.get(workspace_id, {})
        item = ws.get(item_id)
        return copy.deepcopy(item) if item else None

    async def list_items(
        self,
        workspace_id: str,
        status: ItemStatus | None = None,
        priority: ItemPriority | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[WorkItem], dict[str, Any]]:
        t0 = dt.datetime.now(dt.UTC)
        ws = self._items.get(workspace_id, {})
        results = list(ws.values())

        if status:
            results = [x for x in results if x.status == status]
        if priority:
            results = [x for x in results if x.priority == priority]

        # Sort by updated_at descending
        results.sort(key=lambda x: x.updated_at, reverse=True)
        total = len(results)
        paginated = results[offset : offset + limit]

        duration_ms = (dt.datetime.now(dt.UTC) - t0).total_seconds() * 1000.0
        explain_plan = {
            "index_used": "idx_workspace_status_priority",
            "scan_type": "IndexScan",
            "total_rows": total,
            "returned_rows": len(paginated),
            "execution_time_ms": round(duration_ms, 3),
        }
        self._query_plan_log.append(explain_plan)
        return [copy.deepcopy(x) for x in paginated], explain_plan

    async def apply_mutation_atomic(
        self,
        tx: TransactionContext,
        mutation: Mutation,
    ) -> tuple[WorkItem, CDCEvent]:
        ws_id = mutation.workspace_id
        if ws_id not in self._items:
            self._items[ws_id] = {}

        current = self._items[ws_id].get(mutation.item_id)

        if mutation.mutation_type == MutationType.CREATE:
            if current:
                raise ConcurrencyConflictError(
                    f"Item {mutation.item_id} already exists in workspace {ws_id}"
                )
            new_item = WorkItem(
                id=mutation.item_id,
                workspace_id=ws_id,
                project_id=mutation.patch.get("project_id", "default"),
                title=mutation.patch.get("title", "Untitled"),
                description=mutation.patch.get("description", ""),
                status=mutation.patch.get("status", ItemStatus.TODO),
                priority=mutation.patch.get("priority", ItemPriority.MEDIUM),
                assignee_id=mutation.patch.get("assignee_id"),
                version=1,
                metadata=mutation.patch.get("metadata", {}),
            )
            self._items[ws_id][mutation.item_id] = new_item
            cdc_event = CDCEvent(
                sequence_num=self._sequence_counter,
                event_id=f"cdc_{mutation.mutation_id}",
                workspace_id=ws_id,
                entity_id=new_item.id,
                op=MutationType.CREATE,
                before=None,
                after=new_item.model_dump(mode="json"),
                actor=mutation.actor,
                timestamp=mutation.timestamp,
            )
            tx.pending_outbox.append(cdc_event)
            return copy.deepcopy(new_item), cdc_event

        if not current:
            raise NotFoundError(f"Item {mutation.item_id} not found in workspace {ws_id}")

        # Check optimistic concurrency
        if mutation.expected_version is not None and current.version != mutation.expected_version:
            raise ConcurrencyConflictError(
                f"Version conflict on item {mutation.item_id}: expected {mutation.expected_version}, got {current.version}"
            )

        before_state = current.model_dump(mode="json")

        if mutation.mutation_type == MutationType.DELETE:
            del self._items[ws_id][mutation.item_id]
            cdc_event = CDCEvent(
                sequence_num=self._sequence_counter,
                event_id=f"cdc_{mutation.mutation_id}",
                workspace_id=ws_id,
                entity_id=mutation.item_id,
                op=MutationType.DELETE,
                before=before_state,
                after=None,
                actor=mutation.actor,
                timestamp=mutation.timestamp,
            )
            tx.pending_outbox.append(cdc_event)
            return current, cdc_event

        # Apply patch updates
        for field, val in mutation.patch.items():
            if hasattr(current, field) and field not in (
                "id",
                "workspace_id",
                "version",
                "created_at",
            ):
                setattr(current, field, val)

        current.version += 1
        current.updated_at = dt.datetime.now(dt.UTC)

        cdc_event = CDCEvent(
            sequence_num=self._sequence_counter,
            event_id=f"cdc_{mutation.mutation_id}",
            workspace_id=ws_id,
            entity_id=current.id,
            op=mutation.mutation_type,
            before=before_state,
            after=current.model_dump(mode="json"),
            actor=mutation.actor,
            timestamp=mutation.timestamp,
        )
        tx.pending_outbox.append(cdc_event)
        return copy.deepcopy(current), cdc_event

    async def get_outbox_events(self, since_seq: int = 0) -> list[CDCEvent]:
        return [copy.deepcopy(x) for x in self._outbox if x.sequence_num > since_seq]
