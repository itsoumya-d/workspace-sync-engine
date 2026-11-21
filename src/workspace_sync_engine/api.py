from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncGenerator, Dict, List, Optional
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import StreamingResponse

from workspace_sync_engine.audit import UnauthorizedAgentActionError
from workspace_sync_engine.db import ConcurrencyConflictError, NotFoundError
from workspace_sync_engine.engine import WorkspaceSyncEngine
from workspace_sync_engine.models import (
    AuditEvent,
    CDCEvent,
    ItemPriority,
    ItemStatus,
    Mutation,
    WorkItem,
)

app = FastAPI(
    title="Workspace Sync Engine API",
    description="Real-time collaborative work state synchronization, CDC event streaming, and human-agent audit trail infrastructure.",
    version="1.0.0",
)

engine = WorkspaceSyncEngine()


@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check() -> Dict[str, Any]:
    return {
        "status": "healthy",
        "service": "workspace-sync-engine",
        "version": "1.0.0",
        "cache_metrics": engine.cache.metrics,
        "active_subscribers": len(engine.cdc._global_subscribers)
        + sum(len(s) for s in engine.cdc._subscribers.values()),
    }


@app.get(
    "/workspaces/{workspace_id}/items",
    response_model=Dict[str, Any],
    status_code=status.HTTP_200_OK,
)
async def list_items(
    workspace_id: str,
    status_filter: Optional[ItemStatus] = Query(None, alias="status"),
    priority_filter: Optional[ItemPriority] = Query(None, alias="priority"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    items, explain = await engine.list_items(
        workspace_id=workspace_id,
        status=status_filter,
        priority=priority_filter,
        limit=limit,
        offset=offset,
    )
    return {
        "workspace_id": workspace_id,
        "items": [item.model_dump(mode="json") for item in items],
        "explain": explain,
    }


@app.get(
    "/workspaces/{workspace_id}/items/{item_id}",
    response_model=Dict[str, Any],
    status_code=status.HTTP_200_OK,
)
async def get_item(workspace_id: str, item_id: str) -> Dict[str, Any]:
    item = await engine.get_item(workspace_id, item_id)
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Item {item_id} not found in workspace {workspace_id}",
        )
    return item.model_dump(mode="json")


@app.post(
    "/workspaces/{workspace_id}/mutate",
    response_model=Dict[str, Any],
    status_code=status.HTTP_200_OK,
)
async def apply_mutation(workspace_id: str, mutation: Mutation) -> Dict[str, Any]:
    if mutation.workspace_id != workspace_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="workspace_id in path does not match mutation payload",
        )
    try:
        updated, cdc_event, audit_event = await engine.apply_mutation(mutation)
        return {
            "status": "applied",
            "item": updated.model_dump(mode="json"),
            "cdc_event": cdc_event.model_dump(mode="json"),
            "audit_event": audit_event.model_dump(mode="json"),
        }
    except ConcurrencyConflictError as cce:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(cce),
        )
    except NotFoundError as nfe:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(nfe),
        )
    except UnauthorizedAgentActionError as uae:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(uae),
        )


@app.get(
    "/workspaces/{workspace_id}/audit",
    response_model=List[Dict[str, Any]],
    status_code=status.HTTP_200_OK,
)
async def get_audit_trail(
    workspace_id: str,
    item_id: Optional[str] = Query(None),
) -> List[Dict[str, Any]]:
    trail = engine.get_audit_trail(workspace_id, item_id)
    return [ev.model_dump(mode="json") for ev in trail]


@app.get(
    "/workspaces/{workspace_id}/audit/verify",
    response_model=Dict[str, Any],
    status_code=status.HTTP_200_OK,
)
async def verify_audit(workspace_id: str) -> Dict[str, Any]:
    is_valid, count, broken_id = engine.verify_audit_integrity(workspace_id)
    return {
        "workspace_id": workspace_id,
        "is_valid": is_valid,
        "event_count": count,
        "broken_event_id": broken_id,
    }


@app.get("/workspaces/{workspace_id}/events/stream")
async def stream_cdc_events(workspace_id: str) -> StreamingResponse:
    async def event_generator() -> AsyncGenerator[str, None]:
        queue = engine.subscribe_cdc(workspace_id)
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"data: {event.model_dump_json()}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            engine.unsubscribe_cdc(queue, workspace_id)

    return StreamingResponse(event_generator(), media_type="text/event-stream")
