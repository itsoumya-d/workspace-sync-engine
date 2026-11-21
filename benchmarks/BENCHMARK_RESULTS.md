# Benchmark & Performance Report

- **Date:** 2026-11-21
- **Platform:** macOS Darwin 24.3.0 (arm64 Apple Silicon)
- **Runtime:** Python 3.12/3.14 (asyncio / FastAPI)
- **Workload:** 25 concurrent worker threads, 20 iterations each (1,500 total transactional operations: CREATE, READ, UPDATE)

---

## 1. Measured Performance Results

| Metric | Target SLO | Measured Result | Status |
|---|---|---|---|
| **Total Operations** | `1,500 ops` | **1,500 ops** | **COMPLETED** |
| **Total Duration** | `< 10 s` | **4.36 s** | **PASSED** |
| **Throughput** | `> 250 ops/sec` | **344 ops/sec** | **PASSED** |
| **p50 Latency** | `< 10.0 ms` | **6.05 ms** | **PASSED** |
| **Audit Chain Integrity** | `100% Valid` | `100% Valid (1,000/1,000 verified)` | **PASSED** |
| **Zero Lost Updates** | `0% loss` | `0% loss (100% atomic outbox commits)` | **PASSED** |

---

## 2. Concurrency Conflict & Rollback Verification

- 50 simultaneous competing mutations targeting the exact same work item were evaluated for optimistic concurrency handling.
- **Results:**
  - 1 initial mutation succeeded at version 1.
  - Competing concurrent updates detected version skew and triggered exponential backoff retries.
  - 100% of conflicting transactions were either cleanly retried to consecutive version numbers or rolled back with zero orphaned outbox events.
  - Cryptographic audit chain verified: 1,000/1,000 events correctly linked with unbroken SHA-256 hash sequence.
