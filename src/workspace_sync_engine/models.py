from __future__ import annotations

import datetime as dt
import enum
from typing import Any

from pydantic import BaseModel, Field


class ActorType(str, enum.Enum):
    HUMAN = "human"
    AGENT = "agent"
    SYSTEM = "system"


class ItemStatus(str, enum.Enum):
    BACKLOG = "backlog"
    TODO = "todo"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    CANCELLED = "cancelled"


class ItemPriority(str, enum.Enum):
    URGENT = "urgent"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    NONE = "none"


class MutationType(str, enum.Enum):
    CREATE = "create"
    UPDATE = "update"
    TRANSITION = "transition"
    DELETE = "delete"


class Actor(BaseModel):
    id: str
    name: str
    actor_type: ActorType
    role: str = "member"
    authority_token: str | None = None


class WorkItem(BaseModel):
    id: str
    workspace_id: str
    project_id: str
    title: str
    description: str = ""
    status: ItemStatus = ItemStatus.TODO
    priority: ItemPriority = ItemPriority.MEDIUM
    assignee_id: str | None = None
    version: int = 1
    created_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.UTC))
    updated_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.UTC))
    metadata: dict[str, Any] = Field(default_factory=dict)


class Mutation(BaseModel):
    mutation_id: str
    workspace_id: str
    item_id: str
    mutation_type: MutationType
    patch: dict[str, Any] = Field(default_factory=dict)
    expected_version: int | None = None
    actor: Actor
    rationale: str | None = None
    timestamp: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.UTC))


class AuditEvent(BaseModel):
    event_id: str
    workspace_id: str
    item_id: str
    actor_id: str
    actor_type: ActorType
    action: MutationType
    previous_state: dict[str, Any] | None = None
    new_state: dict[str, Any] | None = None
    rationale: str | None = None
    prev_hash: str
    curr_hash: str
    timestamp: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.UTC))


class CDCEvent(BaseModel):
    sequence_num: int
    event_id: str
    workspace_id: str
    entity_type: str = "work_item"
    entity_id: str
    op: MutationType
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    actor: Actor
    timestamp: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.UTC))
