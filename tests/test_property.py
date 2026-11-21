import random
import string

from workspace_sync_engine.audit import DurableAuditLogger
from workspace_sync_engine.models import (
    Actor,
    ActorType,
    Mutation,
    MutationType,
    WorkItem,
)

try:
    from hypothesis import given
    from hypothesis import strategies as st

    @given(
        titles=st.lists(
            st.text(min_size=1, max_size=50).filter(lambda x: "\x00" not in x),
            min_size=1,
            max_size=20,
        )
    )
    def test_property_audit_chain_integrity_hypothesis(titles):
        _run_audit_chain_test(titles)

except ImportError:
    pass


def _run_audit_chain_test(titles):
    logger = DurableAuditLogger()
    actor = Actor(id="human_tester", name="Tester", actor_type=ActorType.HUMAN)
    ws_id = "ws_prop"

    prev_item = None
    for idx, title in enumerate(titles):
        item = WorkItem(
            id="item_prop",
            workspace_id=ws_id,
            project_id="p_prop",
            title=title,
            version=idx + 1,
        )
        mut = Mutation(
            mutation_id=f"mut_{idx}",
            workspace_id=ws_id,
            item_id="item_prop",
            mutation_type=MutationType.CREATE if idx == 0 else MutationType.UPDATE,
            patch={"title": title},
            actor=actor,
        )
        logger.record_mutation(mut, prev_item, item)
        prev_item = item

    is_valid, count, broken_id = logger.verify_audit_chain(ws_id)
    assert is_valid is True
    assert count == len(titles)
    assert broken_id is None


def test_property_generative_random_walk():
    """Generative randomized property testing without hypothesis dependency."""
    for seed in range(10):
        rng = random.Random(seed)
        n = rng.randint(5, 25)
        titles = [
            "".join(rng.choices(string.ascii_letters + string.digits + " _-", k=rng.randint(3, 30)))
            for _ in range(n)
        ]
        _run_audit_chain_test(titles)
