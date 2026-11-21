#!/usr/bin/env bash
set -euo pipefail
echo "Starting smoke test for workspace-sync-engine..."
python3 -c "from workspace_sync_engine.api import app; print('App import smoke test: OK')"
python3 -c "from workspace_sync_engine.engine import WorkspaceSyncEngine; e = WorkspaceSyncEngine(); print('Engine init smoke test: OK')"
echo "Smoke test passed successfully!"
