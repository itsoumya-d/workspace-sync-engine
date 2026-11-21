import pytest

from workspace_sync_engine.audit import (
    DurableAuditLogger,
    UnauthorizedAgentActionError,
)
from workspace_sync_engine.models import (
    Actor,
    ActorType,
    Mutation,
    MutationType,
    WorkItem,
)


def test_agent_authority_validation():
    logger = DurableAuditLogger()

    # Agent without authority token
    rogue_agent = Actor(id="agt_rogue", name="RogueBot", actor_type=ActorType.AGENT)
    mut_rogue = Mutation(
        mutation_id="m_rogue",
        workspace_id="ws_sec",
        item_id="item_sec_1",
        mutation_type=MutationType.CREATE,
        patch={"title": "Unauthorized task"},
        actor=rogue_agent,
        rationale="Trying to act without token",
    )
    with pytest.raises(UnauthorizedAgentActionError, match="lacks a valid authority token"):
        logger.record_mutation(mut_rogue, None, None)

    # Agent without rationale
    valid_agent = Actor(
        id="agt_valid",
        name="ValidBot",
        actor_type=ActorType.AGENT,
        authority_token="valid_secret_token_1234567890",
    )
    mut_no_rationale = Mutation(
        mutation_id="m_no_rat",
        workspace_id="ws_sec",
        item_id="item_sec_1",
        mutation_type=MutationType.CREATE,
        patch={"title": "No rationale"},
        actor=valid_agent,
        rationale="",
    )
    with pytest.raises(UnauthorizedAgentActionError, match="must provide explicit rationale"):
        logger.record_mutation(mut_no_rationale, None, None)


def test_tamper_detection_in_audit_chain():
    logger = DurableAuditLogger()
    human = Actor(id="usr_1", name="Alice", actor_type=ActorType.HUMAN)

    item1 = WorkItem(id="item_1", workspace_id="ws_audit", project_id="p1", title="Init")
    mut1 = Mutation(
        mutation_id="m1",
        workspace_id="ws_audit",
        item_id="item_1",
        mutation_type=MutationType.CREATE,
        patch={"title": "Init"},
        actor=human,
    )
    logger.record_mutation(mut1, None, item1)

    item2 = WorkItem(
        id="item_1", workspace_id="ws_audit", project_id="p1", title="Second", version=2
    )
    mut2 = Mutation(
        mutation_id="m2",
        workspace_id="ws_audit",
        item_id="item_1",
        mutation_type=MutationType.UPDATE,
        patch={"title": "Second"},
        actor=human,
    )
    logger.record_mutation(mut2, item1, item2)

    is_valid, count, broken_id = logger.verify_audit_chain("ws_audit")
    assert is_valid is True
    assert count == 2
    assert broken_id is None

    # Simulate malicious tamper in historical record
    chain = logger._chains["ws_audit"]
    chain[0].new_state["title"] = "Tampered Title"

    is_valid_after_tamper, _, broken_id_after = logger.verify_audit_chain("ws_audit")
    assert is_valid_after_tamper is False
    assert broken_id_after == "aud_m1"
