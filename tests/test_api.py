from fastapi.testclient import TestClient

from workspace_sync_engine.api import app

client = TestClient(app)


def test_api_health():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "workspace-sync-engine"
    assert "cache_metrics" in data


def test_api_crud_and_audit():
    ws_id = "ws_test_api"

    # 1. Create item
    create_payload = {
        "mutation_id": "api_m1",
        "workspace_id": ws_id,
        "item_id": "task_api_1",
        "mutation_type": "create",
        "patch": {
            "title": "API Test Task",
            "description": "Testing through FastAPI endpoints",
            "priority": "high",
            "status": "todo",
        },
        "actor": {
            "id": "usr_web",
            "name": "WebUser",
            "actor_type": "human",
            "role": "admin",
        },
    }
    res_create = client.post(f"/workspaces/{ws_id}/mutate", json=create_payload)
    assert res_create.status_code == 200
    res_data = res_create.json()
    assert res_data["status"] == "applied"
    assert res_data["item"]["id"] == "task_api_1"
    assert res_data["item"]["version"] == 1

    # 2. List items
    res_list = client.get(f"/workspaces/{ws_id}/items")
    assert res_list.status_code == 200
    list_data = res_list.json()
    assert len(list_data["items"]) == 1
    assert list_data["items"][0]["title"] == "API Test Task"
    assert "explain" in list_data

    # 3. Get single item
    res_get = client.get(f"/workspaces/{ws_id}/items/task_api_1")
    assert res_get.status_code == 200
    assert res_get.json()["id"] == "task_api_1"

    # 4. Update item
    update_payload = {
        "mutation_id": "api_m2",
        "workspace_id": ws_id,
        "item_id": "task_api_1",
        "mutation_type": "update",
        "expected_version": 1,
        "patch": {
            "title": "Updated API Task Title",
        },
        "actor": {
            "id": "usr_web",
            "name": "WebUser",
            "actor_type": "human",
            "role": "admin",
        },
    }
    res_update = client.post(f"/workspaces/{ws_id}/mutate", json=update_payload)
    assert res_update.status_code == 200
    assert res_update.json()["item"]["version"] == 2
    assert res_update.json()["item"]["title"] == "Updated API Task Title"

    # 5. Check audit trail
    res_audit = client.get(f"/workspaces/{ws_id}/audit")
    assert res_audit.status_code == 200
    audit_events = res_audit.json()
    assert len(audit_events) == 2

    # 6. Verify audit chain
    res_verify = client.get(f"/workspaces/{ws_id}/audit/verify")
    assert res_verify.status_code == 200
    verify_data = res_verify.json()
    assert verify_data["is_valid"] is True
    assert verify_data["event_count"] == 2
    assert verify_data["broken_event_id"] is None
