#!/usr/bin/env bash
set -euo pipefail
echo "Starting smoke test for workspace-sync-engine..."
PYTHONPATH=src python3 -c "from workspace_sync_engine.api import app; print('App import smoke test: OK')"
PYTHONPATH=src python3 -c "from workspace_sync_engine.engine import WorkspaceSyncEngine; e = WorkspaceSyncEngine(); print('Engine init smoke test: OK')"
PYTHONPATH=src python3 -c "from workspace_sync_engine.lakehouse import LakehouseMedallionSink; s = LakehouseMedallionSink(); print('Lakehouse sink smoke test: OK')"
echo "Smoke test passed successfully!"
