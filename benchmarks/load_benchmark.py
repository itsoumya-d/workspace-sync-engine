import asyncio
import datetime as dt
import json
import statistics
import time
from typing import List

from workspace_sync_engine.engine import WorkspaceSyncEngine
from workspace_sync_engine.models import (
    Actor,
    ActorType,
    ItemPriority,
    ItemStatus,
    Mutation,
    MutationType,
)


async def worker(
    worker_id: int,
    engine: WorkspaceSyncEngine,
    workspace_id: str,
    iterations: int,
    latencies: List[float],
) -> None:
    actor = (
        Actor(
            id=f"agt_{worker_id}",
            name=f"Agent-{worker_id}",
            actor_type=ActorType.AGENT,
            authority_token="auth_secret_token_abcdef123456",
        )
        if worker_id % 2 == 0
        else Actor(id=f"usr_{worker_id}", name=f"Human-{worker_id}", actor_type=ActorType.HUMAN)
    )

    for i in range(iterations):
        item_id = f"item_{worker_id}_{i}"
        t0 = time.perf_counter()

        # 1. Create
        await engine.apply_mutation(
            Mutation(
                mutation_id=f"bench_create_{worker_id}_{i}",
                workspace_id=workspace_id,
                item_id=item_id,
                mutation_type=MutationType.CREATE,
                patch={
                    "title": f"Bench Task {worker_id}-{i}",
                    "priority": ItemPriority.HIGH,
                },
                actor=actor,
                rationale="Automated load benchmark initialization",
            )
        )

        # 2. Read
        await engine.get_item(workspace_id, item_id)

        # 3. Update
        await engine.apply_mutation(
            Mutation(
                mutation_id=f"bench_upd_{worker_id}_{i}",
                workspace_id=workspace_id,
                item_id=item_id,
                mutation_type=MutationType.UPDATE,
                patch={"status": ItemStatus.IN_PROGRESS},
                expected_version=1,
                actor=actor,
                rationale="Bench transition to in_progress",
            )
        )

        lat = (time.perf_counter() - t0) * 1000.0
        latencies.append(lat)


async def run_benchmark(concurrency: int = 50, iterations_per_worker: int = 20):
    print(f"Starting benchmark: {concurrency} workers x {iterations_per_worker} ops...")
    engine = WorkspaceSyncEngine()
    ws_id = "ws_benchmark"
    latencies: List[float] = []

    t_start = time.perf_counter()
    tasks = [
        worker(w_id, engine, ws_id, iterations_per_worker, latencies)
        for w_id in range(concurrency)
    ]
    await asyncio.gather(*tasks)
    total_time = time.perf_counter() - t_start

    total_ops = len(latencies) * 3  # create + read + update = 3 operations per iteration
    throughput = total_ops / total_time

    p50 = statistics.median(latencies)
    p95 = statistics.quantiles(latencies, n=100)[94] if len(latencies) >= 100 else p50
    p99 = statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else p95

    is_valid, count, _ = engine.verify_audit_integrity(ws_id)

    report = {
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "concurrency": concurrency,
        "iterations_per_worker": iterations_per_worker,
        "total_operations": total_ops,
        "total_time_seconds": round(total_time, 3),
        "throughput_ops_sec": round(throughput, 1),
        "p50_latency_ms": round(p50, 3),
        "p95_latency_ms": round(p95, 3),
        "p99_latency_ms": round(p99, 3),
        "slo_target_p99_ms": 15.0,
        "slo_met": p99 < 15.0,
        "audit_chain_valid": is_valid,
        "audit_record_count": count,
        "cache_metrics": engine.cache.metrics,
    }

    print("\nBenchmark Results:")
    print(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    asyncio.run(run_benchmark(concurrency=25, iterations_per_worker=20))
