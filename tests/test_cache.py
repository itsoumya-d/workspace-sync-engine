import asyncio

from workspace_sync_engine.cache import MultiLevelCache
from workspace_sync_engine.engine import WorkspaceSyncEngine
from workspace_sync_engine.models import (
    Actor,
    ActorType,
    Mutation,
    MutationType,
    WorkItem,
)


def test_cache_hit_and_miss():
    async def _run():
        cache = MultiLevelCache(default_ttl_sec=10)

        # Miss
        miss = await cache.get_item("ws_1", "item_missing")
        assert miss is None
        assert cache.metrics["misses"] == 1
        assert cache.metrics["hits"] == 0

        # Put item
        item = WorkItem(
            id="item_cached",
            workspace_id="ws_1",
            project_id="p1",
            title="Cached Issue",
        )
        await cache.put_item("ws_1", item)

        # Hit
        hit = await cache.get_item("ws_1", "item_cached")
        assert hit is not None
        assert hit.title == "Cached Issue"
        assert cache.metrics["hits"] == 1
        assert cache.metrics["hit_ratio"] == 0.5

    asyncio.run(_run())


def test_cdc_invalidation():
    async def _run():
        engine = WorkspaceSyncEngine()
        actor = Actor(id="u1", name="Alice", actor_type=ActorType.HUMAN)

        # 1. Create item
        await engine.apply_mutation(
            Mutation(
                mutation_id="m1",
                workspace_id="ws_cache",
                item_id="item_inv",
                mutation_type=MutationType.CREATE,
                patch={"title": "Original Title"},
                actor=actor,
            )
        )

        # 2. Read to populate cache
        item1 = await engine.get_item("ws_cache", "item_inv")
        assert item1 is not None
        assert item1.title == "Original Title"
        assert engine.cache.metrics["hits"] == 0  # first read from db
        assert engine.cache.metrics["l1_size"] == 1

        # 3. Read again -> cache hit
        item2 = await engine.get_item("ws_cache", "item_inv")
        assert item2 is not None
        assert engine.cache.metrics["hits"] == 1

        # 4. Mutate item -> CDC invalidates cache
        await engine.apply_mutation(
            Mutation(
                mutation_id="m2",
                workspace_id="ws_cache",
                item_id="item_inv",
                mutation_type=MutationType.UPDATE,
                patch={"title": "Modified Title"},
                expected_version=1,
                actor=actor,
            )
        )
        assert engine.cache.metrics["invalidations"] >= 1

        # 5. Read again -> should see fresh data from db and update cache
        item3 = await engine.get_item("ws_cache", "item_inv")
        assert item3 is not None
        assert item3.title == "Modified Title"
        assert item3.version == 2

    asyncio.run(_run())
