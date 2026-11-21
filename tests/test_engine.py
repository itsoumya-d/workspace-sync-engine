import asyncio

import pytest

from workspace_sync_engine.db import ConcurrencyConflictError
from workspace_sync_engine.engine import WorkspaceSyncEngine
from workspace_sync_engine.models import (
    Actor,
    ActorType,
    ItemPriority,
    ItemStatus,
    Mutation,
    MutationType,
)


def test_create_and_get_item():
    async def _run():
        engine = WorkspaceSyncEngine()
        actor = Actor(id="u1", name="Dev Alice", actor_type=ActorType.HUMAN)

        create_mut = Mutation(
            mutation_id="m1",
            workspace_id="ws_1",
            item_id="item_1",
            mutation_type=MutationType.CREATE,
            patch={
                "title": "Setup PostgreSQL CDC",
                "description": "Configure outbox and logical decoding",
                "priority": ItemPriority.HIGH,
            },
            actor=actor,
        )

        created, cdc_ev, _audit_ev = await engine.apply_mutation(create_mut)
        assert created.id == "item_1"
        assert created.version == 1
        assert created.title == "Setup PostgreSQL CDC"
        assert created.priority == ItemPriority.HIGH
        assert cdc_ev.op == MutationType.CREATE

        # Read back through engine
        fetched = await engine.get_item("ws_1", "item_1")
        assert fetched is not None
        assert fetched.id == "item_1"
        assert fetched.version == 1

    asyncio.run(_run())


def test_optimistic_concurrency_conflict():
    async def _run():
        engine = WorkspaceSyncEngine()
        actor = Actor(id="u1", name="Dev Alice", actor_type=ActorType.HUMAN)

        # Create item (version 1)
        await engine.apply_mutation(
            Mutation(
                mutation_id="m1",
                workspace_id="ws_1",
                item_id="item_concurrency",
                mutation_type=MutationType.CREATE,
                patch={"title": "Concurrent Task"},
                actor=actor,
            )
        )

        # Mutate to version 2
        await engine.apply_mutation(
            Mutation(
                mutation_id="m2",
                workspace_id="ws_1",
                item_id="item_concurrency",
                mutation_type=MutationType.UPDATE,
                patch={"title": "Updated title by Alice"},
                expected_version=1,
                actor=actor,
            )
        )

        # Outdated mutation expecting version 1 should fail
        actor2 = Actor(id="u2", name="Dev Bob", actor_type=ActorType.HUMAN)
        stale_mut = Mutation(
            mutation_id="m3",
            workspace_id="ws_1",
            item_id="item_concurrency",
            mutation_type=MutationType.UPDATE,
            patch={"title": "Stale update by Bob"},
            expected_version=1,
            actor=actor2,
        )

        with pytest.raises(ConcurrencyConflictError):
            # Disable auto-retry to assert explicit conflict
            await engine.apply_mutation(stale_mut, max_retries=1)

    asyncio.run(_run())


def test_item_transition_and_audit():
    async def _run():
        engine = WorkspaceSyncEngine()
        human = Actor(id="u1", name="Alice", actor_type=ActorType.HUMAN)
        agent = Actor(
            id="bot_1",
            name="TriageAgent",
            actor_type=ActorType.AGENT,
            authority_token="valid_token_secret_12345678",
        )

        await engine.apply_mutation(
            Mutation(
                mutation_id="m1",
                workspace_id="ws_triage",
                item_id="task_1",
                mutation_type=MutationType.CREATE,
                patch={"title": "Investigate connection pool spike"},
                actor=human,
            )
        )

        # Agent transitions to IN_PROGRESS with rationale
        trans_mut = Mutation(
            mutation_id="m2",
            workspace_id="ws_triage",
            item_id="task_1",
            mutation_type=MutationType.TRANSITION,
            patch={"status": ItemStatus.IN_PROGRESS},
            expected_version=1,
            actor=agent,
            rationale="Automated triage detected active pg_stat_activity alert",
        )
        updated, _cdc, aud = await engine.apply_mutation(trans_mut)
        assert updated.status == ItemStatus.IN_PROGRESS
        assert updated.version == 2
        assert aud.actor_type == ActorType.AGENT
        assert aud.rationale == "Automated triage detected active pg_stat_activity alert"

        # Verify audit integrity
        is_valid, count, broken = engine.verify_audit_integrity("ws_triage")
        assert is_valid is True
        assert count == 2
        assert broken is None

    asyncio.run(_run())
