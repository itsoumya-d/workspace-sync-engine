from __future__ import annotations

import datetime as dt

from workspace_sync_engine.lakehouse import LakehouseMedallionSink
from workspace_sync_engine.models import Actor, ActorType, CDCEvent, MutationType


def _make_actor(actor_id: str = "eng_1", actor_type: ActorType = ActorType.HUMAN) -> Actor:
    return Actor(id=actor_id, name="Test User", actor_type=actor_type)


def test_bronze_ingestion_and_partitioning():
    sink = LakehouseMedallionSink()
    actor = _make_actor()
    events = [
        CDCEvent(
            sequence_num=1,
            event_id="ev_1",
            workspace_id="ws_main",
            entity_id="task_101",
            op=MutationType.CREATE,
            after={"title": "Setup Lakehouse Pipeline", "status": "todo", "priority": "high"},
            actor=actor,
        ),
        CDCEvent(
            sequence_num=2,
            event_id="ev_2",
            workspace_id="ws_main",
            entity_id="task_102",
            op=MutationType.CREATE,
            after={"title": "Ingest Telemetry Data", "status": "in_progress", "priority": "medium"},
            actor=actor,
        ),
    ]

    bronze_batch = sink.ingest_bronze(events)
    assert len(bronze_batch) == 2
    assert len(sink.bronze_store) == 2
    assert bronze_batch[0].partition_date == dt.datetime.now(dt.UTC).strftime("%Y-%m-%d")
    assert bronze_batch[0].payload["title"] == "Setup Lakehouse Pipeline"
    assert bronze_batch[1].sequence_num == 2


def test_silver_upsert_merge():
    sink = LakehouseMedallionSink()
    actor = _make_actor()

    # Create event
    ev_create = CDCEvent(
        sequence_num=10,
        event_id="ev_10",
        workspace_id="ws_delta",
        entity_id="item_a",
        op=MutationType.CREATE,
        after={"title": "Initial Title", "status": "todo", "version": 1},
        actor=actor,
    )
    b_batch = sink.ingest_bronze([ev_create])
    rep1 = sink.merge_silver(b_batch)

    assert rep1.records_processed == 1
    assert rep1.records_promoted == 1
    key = ("ws_delta", "item_a")
    assert key in sink.silver_table
    assert sink.silver_table[key].title == "Initial Title"
    assert sink.silver_table[key].version == 1

    # Update event
    ev_update = CDCEvent(
        sequence_num=11,
        event_id="ev_11",
        workspace_id="ws_delta",
        entity_id="item_a",
        op=MutationType.UPDATE,
        after={"title": "Updated Delta Title", "status": "done", "version": 2},
        actor=actor,
    )
    b_batch2 = sink.ingest_bronze([ev_update])
    rep2 = sink.merge_silver(b_batch2)

    assert rep2.records_promoted == 1
    assert sink.silver_table[key].title == "Updated Delta Title"
    assert sink.silver_table[key].status == "done"
    assert sink.silver_table[key].version == 2
    assert sink.silver_table[key].last_sequence_num == 11


def test_silver_out_of_order_sequence_resolution():
    sink = LakehouseMedallionSink()
    actor = _make_actor()

    # Newer event arrives first
    ev_newer = CDCEvent(
        sequence_num=50,
        event_id="ev_50",
        workspace_id="ws_ooo",
        entity_id="item_ooo",
        op=MutationType.UPDATE,
        after={"title": "State at seq 50", "status": "done"},
        actor=actor,
    )
    sink.merge_silver(sink.ingest_bronze([ev_newer]))

    # Older event arrives later (out of order network delay)
    ev_older = CDCEvent(
        sequence_num=45,
        event_id="ev_45",
        workspace_id="ws_ooo",
        entity_id="item_ooo",
        op=MutationType.UPDATE,
        after={"title": "Stale State at seq 45", "status": "todo"},
        actor=actor,
    )
    rep_stale = sink.merge_silver(sink.ingest_bronze([ev_older]))

    # Must be quarantined/rejected to preserve monotonic consistency
    assert rep_stale.records_quarantined == 1
    assert rep_stale.records_promoted == 0
    assert sink.silver_table[("ws_ooo", "item_ooo")].title == "State at seq 50"


def test_silver_tombstone_deletion():
    sink = LakehouseMedallionSink()
    actor = _make_actor()

    ev_create = CDCEvent(
        sequence_num=1,
        event_id="ev_c",
        workspace_id="ws_del",
        entity_id="item_del",
        op=MutationType.CREATE,
        after={"title": "To be removed", "status": "todo"},
        actor=actor,
    )
    sink.merge_silver(sink.ingest_bronze([ev_create]))

    ev_delete = CDCEvent(
        sequence_num=2,
        event_id="ev_d",
        workspace_id="ws_del",
        entity_id="item_del",
        op=MutationType.DELETE,
        before={"title": "To be removed", "status": "todo"},
        actor=actor,
    )
    sink.merge_silver(sink.ingest_bronze([ev_delete]))

    record = sink.silver_table[("ws_del", "item_del")]
    assert record.is_deleted is True
    assert record.last_sequence_num == 2


def test_data_quality_quarantine_dlq():
    sink = LakehouseMedallionSink()
    actor = _make_actor()

    # Corrupt event missing entity_id
    corrupt_ev = CDCEvent(
        sequence_num=99,
        event_id="ev_corrupt",
        workspace_id="ws_bad",
        entity_id="",
        op=MutationType.CREATE,
        after={"title": "Broken"},
        actor=actor,
    )
    b_batch = sink.ingest_bronze([corrupt_ev])
    report = sink.merge_silver(b_batch)

    assert report.records_quarantined == 1
    assert len(sink.quarantine_dlq) == 1
    assert "Missing entity or workspace ID" in report.violations[0]


def test_gold_metrics_aggregation():
    sink = LakehouseMedallionSink()
    human = _make_actor("human_1", ActorType.HUMAN)
    agent = _make_actor("agent_ai", ActorType.AGENT)

    events = [
        CDCEvent(
            sequence_num=1,
            event_id="e1",
            workspace_id="ws_kpi",
            entity_id="i1",
            op=MutationType.CREATE,
            after={"title": "Task 1"},
            actor=human,
        ),
        CDCEvent(
            sequence_num=2,
            event_id="e2",
            workspace_id="ws_kpi",
            entity_id="i2",
            op=MutationType.CREATE,
            after={"title": "Task 2"},
            actor=agent,
        ),
        CDCEvent(
            sequence_num=3,
            event_id="e3",
            workspace_id="ws_kpi",
            entity_id="i1",
            op=MutationType.DELETE,
            actor=human,
        ),
    ]

    sink.merge_silver(sink.ingest_bronze(events))
    gold = sink.compute_gold_metrics()

    assert "ws_kpi" in gold
    m = gold["ws_kpi"]
    assert m.total_entities == 2
    assert m.active_entities == 1
    assert m.deleted_entities == 1
    assert m.human_mutations == 2
    assert m.agent_mutations == 1


def test_databricks_sql_ddl_generation():
    ddl = LakehouseMedallionSink.generate_databricks_sql_ddl()
    assert "CREATE TABLE IF NOT EXISTS bronze_cdc_events" in ddl
    assert "CREATE TABLE IF NOT EXISTS silver_workspace_items" in ddl
    assert "USING DELTA" in ddl
    assert "MERGE INTO silver_workspace_items" in ddl
    assert "CLUSTER BY (workspace_id, entity_id)" in ddl
