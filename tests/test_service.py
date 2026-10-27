import pytest

fastapi = pytest.importorskip("fastapi")
TestClient = pytest.importorskip("fastapi.testclient").TestClient

from orchestrator.service import create_app  # noqa: E402


@pytest.fixture()
def client():
    return TestClient(create_app())


def test_list_dags(client):
    resp = client.get("/dags")
    assert resp.status_code == 200
    ids = {d["dag_id"] for d in resp.json()}
    assert {"daily_orders", "retry_demo"} <= ids


def test_trigger_and_fetch_run(client):
    resp = client.post("/dags/daily_orders/trigger", json={})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "succeeded"
    assert body["tasks"]["load"]["status"] == "succeeded"
    assert any(e["dataset"] == "orders" for e in body["lineage"])

    fetched = client.get(f"/runs/{body['run_id']}")
    assert fetched.status_code == 200
    assert fetched.json()["run_id"] == body["run_id"]


def test_unknown_dag_returns_404(client):
    assert client.post("/dags/nope/trigger", json={}).status_code == 404
    assert client.get("/runs/nope").status_code == 404
