import asyncio

from workspace_sync_engine.cdc import ChangeDataCaptureStreamer
from workspace_sync_engine.db import DatabaseSession
from workspace_sync_engine.models import (
    Actor,
    ActorType,
    ItemPriority,
    Mutation,
    MutationType,
)


def test_cdc_pipeline_streaming_throughput():
    async def _run():
        db = DatabaseSession()
        cdc = ChangeDataCaptureStreamer(db)
        queue = cdc.subscribe("ws_perf")
        actor = Actor(id="human_1", name="Engineer", actor_type=ActorType.HUMAN)

        async with db.transaction() as tx:
            for i in range(5):
                mut = Mutation(
                    mutation_id=f"mut_{i}",
                    workspace_id="ws_perf",
                    item_id=f"item_{i}",
                    mutation_type=MutationType.CREATE,
                    patch={"title": f"Task {i}", "priority": ItemPriority.MEDIUM},
                    expected_version=None,
                    actor=actor,
                )
                await db.apply_mutation_atomic(tx, mut)

        assert len(db._outbox) == 5
        events = await db.get_outbox_events(since_seq=0)
        for event in events:
            await cdc.emit_direct(event)

        assert queue.qsize() == 5
        received = []
        while not queue.empty():
            received.append(queue.get_nowait())

        assert len(received) == 5
        assert received[0].entity_id == "item_0"
        cdc.unsubscribe(queue, "ws_perf")

    asyncio.run(_run())
