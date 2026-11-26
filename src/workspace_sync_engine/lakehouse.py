from __future__ import annotations

import datetime as dt
from typing import Any

from pydantic import BaseModel, Field

from workspace_sync_engine.models import ActorType, CDCEvent, MutationType


class BronzeRecord(BaseModel):
    """Immutable raw event record ingested into the Delta Bronze layer."""

    sequence_num: int
    event_id: str
    workspace_id: str
    entity_id: str
    op: MutationType
    payload: dict[str, Any]
    actor_id: str
    actor_type: ActorType
    source_timestamp: dt.datetime
    ingestion_timestamp: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.UTC))
    partition_date: str = Field(
        default_factory=lambda: dt.datetime.now(dt.UTC).strftime("%Y-%m-%d")
    )


class SilverRecord(BaseModel):
    """Conformed, deduplicated entity state record in the Delta Silver layer."""

    workspace_id: str
    entity_id: str
    title: str = ""
    status: str = "todo"
    priority: str = "medium"
    version: int = 1
    is_deleted: bool = False
    last_sequence_num: int
    last_actor_id: str
    last_actor_type: ActorType
    updated_at: dt.datetime


class GoldMetrics(BaseModel):
    """Aggregated business activity metrics in the Delta Gold layer."""

    workspace_id: str
    total_entities: int = 0
    active_entities: int = 0
    deleted_entities: int = 0
    human_mutations: int = 0
    agent_mutations: int = 0
    system_mutations: int = 0
    last_event_timestamp: dt.datetime | None = None


class DataQualityReport(BaseModel):
    """Data quality validation summary for a micro-batch."""

    records_processed: int = 0
    records_promoted: int = 0
    records_quarantined: int = 0
    violations: list[str] = Field(default_factory=list)


class LakehouseMedallionSink:
    """Manages Bronze -> Silver -> Gold Medallion Delta Lake pipeline processing."""

    def __init__(self) -> None:
        self.bronze_store: list[BronzeRecord] = []
        self.silver_table: dict[tuple[str, str], SilverRecord] = {}
        self.quarantine_dlq: list[dict[str, Any]] = []

    def ingest_bronze(self, events: list[CDCEvent]) -> list[BronzeRecord]:
        """Ingests raw CDC outbox events into immutable Bronze records."""
        bronze_batch: list[BronzeRecord] = []
        for ev in events:
            rec = BronzeRecord(
                sequence_num=ev.sequence_num,
                event_id=ev.event_id,
                workspace_id=ev.workspace_id,
                entity_id=ev.entity_id,
                op=ev.op,
                payload=ev.after if ev.after is not None else (ev.before or {}),
                actor_id=ev.actor.id,
                actor_type=ev.actor.actor_type,
                source_timestamp=ev.timestamp,
            )
            bronze_batch.append(rec)
            self.bronze_store.append(rec)
        return bronze_batch

    def merge_silver(self, batch: list[BronzeRecord]) -> DataQualityReport:
        """Applies Delta Lake MERGE semantics (upsert/delete) to conformed Silver state."""
        report = DataQualityReport(records_processed=len(batch))

        for rec in batch:
            # Data quality checks
            if not rec.entity_id or not rec.workspace_id:
                report.records_quarantined += 1
                report.violations.append(f"Missing entity or workspace ID in event {rec.event_id}")
                self.quarantine_dlq.append(rec.model_dump())
                continue

            key = (rec.workspace_id, rec.entity_id)
            existing = self.silver_table.get(key)

            # Idempotency / Monotonic sequence check: ignore out-of-order stale events
            if existing and rec.sequence_num <= existing.last_sequence_num:
                report.records_quarantined += 1
                report.violations.append(
                    f"Stale sequence {rec.sequence_num} <= {existing.last_sequence_num} for entity {rec.entity_id}"
                )
                continue

            if rec.op == MutationType.DELETE:
                if existing:
                    existing.is_deleted = True
                    existing.last_sequence_num = rec.sequence_num
                    existing.last_actor_id = rec.actor_id
                    existing.last_actor_type = rec.actor_type
                    existing.updated_at = rec.source_timestamp
                else:
                    self.silver_table[key] = SilverRecord(
                        workspace_id=rec.workspace_id,
                        entity_id=rec.entity_id,
                        is_deleted=True,
                        last_sequence_num=rec.sequence_num,
                        last_actor_id=rec.actor_id,
                        last_actor_type=rec.actor_type,
                        updated_at=rec.source_timestamp,
                    )
            else:
                # Upsert (CREATE, UPDATE, TRANSITION)
                payload = rec.payload
                version = int(payload.get("version", existing.version + 1 if existing else 1))
                title = str(payload.get("title", existing.title if existing else ""))
                status = str(payload.get("status", existing.status if existing else "todo"))
                priority = str(payload.get("priority", existing.priority if existing else "medium"))

                self.silver_table[key] = SilverRecord(
                    workspace_id=rec.workspace_id,
                    entity_id=rec.entity_id,
                    title=title,
                    status=status,
                    priority=priority,
                    version=version,
                    is_deleted=False,
                    last_sequence_num=rec.sequence_num,
                    last_actor_id=rec.actor_id,
                    last_actor_type=rec.actor_type,
                    updated_at=rec.source_timestamp,
                )

            report.records_promoted += 1

        return report

    def compute_gold_metrics(self) -> dict[str, GoldMetrics]:
        """Computes Gold layer workspace analytics and operational KPIs."""
        metrics: dict[str, GoldMetrics] = {}

        # Scan Silver for current entity states
        for (ws_id, _), s_rec in self.silver_table.items():
            if ws_id not in metrics:
                metrics[ws_id] = GoldMetrics(workspace_id=ws_id)
            m = metrics[ws_id]
            m.total_entities += 1
            if s_rec.is_deleted:
                m.deleted_entities += 1
            else:
                m.active_entities += 1

        # Scan Bronze for mutation actor distributions
        for b_rec in self.bronze_store:
            ws_id = b_rec.workspace_id
            if ws_id not in metrics:
                metrics[ws_id] = GoldMetrics(workspace_id=ws_id)
            m = metrics[ws_id]
            if b_rec.actor_type == ActorType.HUMAN:
                m.human_mutations += 1
            elif b_rec.actor_type == ActorType.AGENT:
                m.agent_mutations += 1
            elif b_rec.actor_type == ActorType.SYSTEM:
                m.system_mutations += 1

            if m.last_event_timestamp is None or b_rec.source_timestamp > m.last_event_timestamp:
                m.last_event_timestamp = b_rec.source_timestamp

        return metrics

    @staticmethod
    def generate_databricks_sql_ddl() -> str:
        """Returns production Databricks Delta Lake DDL and MERGE specifications."""
        return """-- Databricks Delta Lake Medallion DDL & MERGE Strategy
CREATE TABLE IF NOT EXISTS bronze_cdc_events (
    sequence_num BIGINT NOT NULL,
    event_id STRING NOT NULL,
    workspace_id STRING NOT NULL,
    entity_id STRING NOT NULL,
    op STRING NOT NULL,
    payload STRING,
    actor_id STRING,
    actor_type STRING,
    source_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    partition_date DATE
)
USING DELTA
PARTITIONED BY (partition_date)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

CREATE TABLE IF NOT EXISTS silver_workspace_items (
    workspace_id STRING NOT NULL,
    entity_id STRING NOT NULL,
    title STRING,
    status STRING,
    priority STRING,
    version INT,
    is_deleted BOOLEAN,
    last_sequence_num BIGINT,
    last_actor_id STRING,
    last_actor_type STRING,
    updated_at TIMESTAMP
)
USING DELTA
CLUSTER BY (workspace_id, entity_id);

-- Delta Lake MERGE Specification for Incremental Upserts
MERGE INTO silver_workspace_items AS target
USING (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY workspace_id, entity_id 
            ORDER BY sequence_num DESC
        ) as rn
        FROM bronze_cdc_events
    ) WHERE rn = 1
) AS source
ON target.workspace_id = source.workspace_id AND target.entity_id = source.entity_id
WHEN MATCHED AND source.sequence_num > target.last_sequence_num AND source.op = 'delete' THEN
    UPDATE SET target.is_deleted = true, target.last_sequence_num = source.sequence_num, target.updated_at = source.source_timestamp
WHEN MATCHED AND source.sequence_num > target.last_sequence_num THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *;
"""
