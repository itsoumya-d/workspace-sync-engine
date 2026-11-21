import pytest
from workspace_sync_engine.models import (
    Actor,
    ActorType,
    AuditEvent,
    CDCEvent,
    ItemPriority,
    ItemStatus,
    Mutation,
    MutationType,
    WorkItem,
)


def test_actor_instantiation():
    human = Actor(id="usr_1", name="Alice", actor_type=ActorType.HUMAN, role="admin")
    assert human.actor_type == ActorType.HUMAN
    assert human.authority_token is None

    agent = Actor(
        id="agt_1",
        name="AutoTriageAgent",
        actor_type=ActorType.AGENT,
        authority_token="auth_token_987654321012345",
    )
    assert agent.actor_type == ActorType.AGENT
    assert agent.authority_token.startswith("auth_token_")


def test_work_item_defaults():
    item = WorkItem(
        id="item_101",
        workspace_id="ws_eng",
        project_id="proj_core",
        title="Implement CDC outbox",
    )
    assert item.status == ItemStatus.TODO
    assert item.priority == ItemPriority.MEDIUM
    assert item.version == 1
    assert item.description == ""


def test_mutation_creation():
    actor = Actor(id="usr_2", name="Bob", actor_type=ActorType.HUMAN)
    mutation = Mutation(
        mutation_id="mut_1",
        workspace_id="ws_eng",
        item_id="item_101",
        mutation_type=MutationType.TRANSITION,
        patch={"status": ItemStatus.IN_PROGRESS},
        expected_version=1,
        actor=actor,
    )
    assert mutation.mutation_type == MutationType.TRANSITION
    assert mutation.patch["status"] == ItemStatus.IN_PROGRESS
    assert mutation.expected_version == 1
