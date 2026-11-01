"""Starter test — passes out of the box. Ingest tests live in test_ingest_api.py, test_normalize.py, ..."""


def test_health_endpoint_responds(client):
    r = client.get("/health")
    assert r.status_code in (200, 503)          # 503 only if Postgres is not running
    assert r.json()["status"] in ("UP", "DEGRADED")
