import asyncio
from workspace_sync_engine.db import DatabaseSession
from workspace_sync_engine.cdc import ChangeDataCaptureStreamer
from workspace_sync_engine.models import Actor, ActorType, ItemPriority, ItemStatus, Mutation, MutationType

def test_cdc_pipeline_streaming_throughput():
    async def _run():
        db = DatabaseSession()
        cdc = ChangeDataCaptureStreamer(db)
        actor = Actor(id="human_1", name="Engineer", actor_type=ActorType.HUMAN)
        
        # Test creating multiple mutations and streaming them
        events_received = []
        async def subscriber(event):
            events_received.append(event)
            
        cdc.subscribe_workspace("ws_perf", subscriber)
        
        for i in range(10):
            mut = Mutation(
                workspace_id="ws_perf",
                item_id=f"item_{i}",
                mutation_type=MutationType.CREATE,
                patch={"title": f"Task {i}", "priority": ItemPriority.MEDIUM},
                expected_version=None,
                actor=actor
            )
            item, ev = await db.apply_mutation(mut)
            assert ev is not None
            
        dispatched = await cdc.flush_outbox()
        assert dispatched == 10
        assert len(events_received) == 10
        assert len(db._outbox) == 0

    asyncio.run(_run())
