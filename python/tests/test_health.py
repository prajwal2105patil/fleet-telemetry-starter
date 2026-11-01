"""Starter test — passes out of the box. Add your own tests next to it (test_ingest_api.py, test_normalize.py, ...)."""


def test_health_endpoint_responds(client):
    r = client.get("/health")
    assert r.status_code in (200, 503)          # 503 only if Postgres is not running
    assert r.json()["status"] in ("UP", "DEGRADED")


def test_ingest_is_not_implemented_yet(client, heartbeat):
    # Delete this test once M1 is done, and replace it with real ones:
    #   valid heartbeat -> 202 and one message published, keyed by VIN
    #   each invalid field -> 400 with {"error": "validation_failed", "details": [...]} and nothing published
    assert client.post("/v1/heartbeats", json=heartbeat).status_code == 501
