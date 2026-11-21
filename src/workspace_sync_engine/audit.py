from __future__ import annotations

import hashlib
import json
from typing import Dict, List, Optional, Tuple

from workspace_sync_engine.models import (
    Actor,
    ActorType,
    AuditEvent,
    Mutation,
    WorkItem,
)


class UnauthorizedAgentActionError(Exception):
    """Raised when an AI agent attempts a mutation without valid authority credentials."""
    pass


class DurableAuditLogger:
    """Tamper-evident, hash-chained audit log for human and AI agent actions."""

    GENESIS_HASH = "0" * 64

    def __init__(self) -> None:
        self._chains: Dict[str, List[AuditEvent]] = {}  # workspace_id -> list of events

    def _compute_hash(self, event_dict: Dict[str, Any], prev_hash: str) -> str:
        payload = f"{prev_hash}:{event_dict['event_id']}:{event_dict['workspace_id']}:{event_dict['item_id']}:{event_dict['actor_id']}:{event_dict['action']}:{json.dumps(event_dict['new_state'], sort_keys=True)}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def record_mutation(
        self,
        mutation: Mutation,
        previous_item: Optional[WorkItem],
        new_item: Optional[WorkItem],
    ) -> AuditEvent:
        # Validate agent authority
        if mutation.actor.actor_type == ActorType.AGENT:
            if not mutation.actor.authority_token or len(mutation.actor.authority_token) < 16:
                raise UnauthorizedAgentActionError(
                    f"Agent {mutation.actor.id} lacks a valid authority token (must be >= 16 characters)"
                )
            if not mutation.rationale or len(mutation.rationale.strip()) < 10:
                raise UnauthorizedAgentActionError(
                    f"Agent {mutation.actor.id} must provide explicit rationale (>= 10 chars) for auditability"
                )

        ws_id = mutation.workspace_id
        if ws_id not in self._chains:
            self._chains[ws_id] = []

        prev_hash = (
            self._chains[ws_id][-1].curr_hash
            if self._chains[ws_id]
            else self.GENESIS_HASH
        )

        event_id = f"aud_{mutation.mutation_id}"
        prev_state = previous_item.model_dump(mode="json") if previous_item else None
        curr_state = new_item.model_dump(mode="json") if new_item else None

        raw_event = {
            "event_id": event_id,
            "workspace_id": ws_id,
            "item_id": mutation.item_id,
            "actor_id": mutation.actor.id,
            "action": mutation.mutation_type,
            "new_state": curr_state,
        }
        curr_hash = self._compute_hash(raw_event, prev_hash)

        audit_event = AuditEvent(
            event_id=event_id,
            workspace_id=ws_id,
            item_id=mutation.item_id,
            actor_id=mutation.actor.id,
            actor_type=mutation.actor.actor_type,
            action=mutation.mutation_type,
            previous_state=prev_state,
            new_state=curr_state,
            rationale=mutation.rationale,
            prev_hash=prev_hash,
            curr_hash=curr_hash,
            timestamp=mutation.timestamp,
        )
        self._chains[ws_id].append(audit_event)
        return audit_event

    def get_audit_trail(
        self, workspace_id: str, item_id: Optional[str] = None
    ) -> List[AuditEvent]:
        chain = self._chains.get(workspace_id, [])
        if item_id:
            return [x for x in chain if x.item_id == item_id]
        return list(chain)

    def verify_audit_chain(self, workspace_id: str) -> Tuple[bool, int, Optional[str]]:
        """Verifies cryptographic hash chain integrity for the given workspace.
        Returns: (is_valid, record_count, broken_event_id_or_none)
        """
        chain = self._chains.get(workspace_id, [])
        if not chain:
            return True, 0, None

        expected_prev = self.GENESIS_HASH
        for event in chain:
            if event.prev_hash != expected_prev:
                return False, len(chain), event.event_id

            raw_event = {
                "event_id": event.event_id,
                "workspace_id": event.workspace_id,
                "item_id": event.item_id,
                "actor_id": event.actor_id,
                "action": event.action,
                "new_state": event.new_state,
            }
            computed = self._compute_hash(raw_event, expected_prev)
            if computed != event.curr_hash:
                return False, len(chain), event.event_id

            expected_prev = event.curr_hash

        return True, len(chain), None
