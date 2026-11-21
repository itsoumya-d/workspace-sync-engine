import asyncio
import pytest
from workspace_sync_engine.cdc import ChangeDataCaptureStreamer
from workspace_sync_engine.db import DatabaseSession
from workspace_sync_engine.engine import WorkspaceSyncEngine
from workspace_sync_engine.models import (
    Actor,
    ActorType,
    CDCEvent,
    Mutation,
    MutationType,
)


def test_cdc_subscription_filtering():
    async def _run():
        engine = WorkspaceSyncEngine()
        actor = Actor(id="u1", name="Alice", actor_type=ActorType.HUMAN)

        queue_ws1 = engine.subscribe_cdc("ws_alpha")
        queue_ws2 = engine.subscribe_cdc("ws_beta")
        queue_global = engine.subscribe_cdc(None)

        # Mutate ws_alpha
        await engine.apply_mutation(
            Mutation(
                mutation_id="m1",
                workspace_id="ws_alpha",
                item_id="item_alpha_1",
                mutation_type=MutationType.CREATE,
                patch={"title": "Alpha item"},
                actor=actor,
            )
        )

        # Mutate ws_beta
        await engine.apply_mutation(
            Mutation(
                mutation_id="m2",
                workspace_id="ws_beta",
                item_id="item_beta_1",
                mutation_type=MutationType.CREATE,
                patch={"title": "Beta item"},
                actor=actor,
            )
        )

        # Check ws_alpha queue received only ws_alpha event
        ev1 = queue_ws1.get_nowait()
        assert ev1.workspace_id == "ws_alpha"
        assert ev1.entity_id == "item_alpha_1"
        assert queue_ws1.empty()

        # Check ws_beta queue received only ws_beta event
        ev2 = queue_ws2.get_nowait()
        assert ev2.workspace_id == "ws_beta"
        assert ev2.entity_id == "item_beta_1"
        assert queue_ws2.empty()

        # Global queue received both in order
        gev1 = queue_global.get_nowait()
        gev2 = queue_global.get_nowait()
        assert gev1.workspace_id == "ws_alpha"
        assert gev2.workspace_id == "ws_beta"

        # Cleanup
        engine.unsubscribe_cdc(queue_ws1, "ws_alpha")
        engine.unsubscribe_cdc(queue_ws2, "ws_beta")
        engine.unsubscribe_cdc(queue_global, None)

    asyncio.run(_run())


def test_cdc_poller_loop():
    async def _run():
        db = DatabaseSession()
        streamer = ChangeDataCaptureStreamer(db)
        queue = streamer.subscribe("ws_test")

        await streamer.start(poll_interval_ms=10)

        actor = Actor(id="u1", name="Alice", actor_type=ActorType.HUMAN)
        async with db.transaction() as tx:
            await db.apply_mutation_atomic(
                tx,
                Mutation(
                    mutation_id="m_poller",
                    workspace_id="ws_test",
                    item_id="item_poller",
                    mutation_type=MutationType.CREATE,
                    patch={"title": "Polled CDC test"},
                    actor=actor,
                ),
            )

        # Wait for poller to pick up outbox
        event = await asyncio.wait_for(queue.get(), timeout=2.0)
        assert event.workspace_id == "ws_test"
        assert event.entity_id == "item_poller"

        await streamer.stop()

    asyncio.run(_run())
