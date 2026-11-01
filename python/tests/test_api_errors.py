"""The 400 error body is part of the API contract: {"error": "validation_failed", "details": [{"field", "message"}]}."""


def post(client, **kwargs):
    return client.post("/v1/heartbeats", **kwargs)


def test_missing_field_is_reported_by_field_name(client, heartbeat):
    del heartbeat["vin"]
    r = post(client, json=heartbeat)
    assert r.status_code == 400
    assert r.json()["error"] == "validation_failed"
    assert {"field": "vin", "message": "Field required"} in r.json()["details"]


def test_malformed_json_is_400_and_names_the_body_not_a_character_offset(client):
    # Pydantic reports the character offset of the JSON syntax error in `loc`; that is not a field name.
    r = post(client, content="{bad", headers={"Content-Type": "application/json"})
    assert r.status_code == 400
    body = r.json()
    assert body["error"] == "validation_failed"
    assert [d["field"] for d in body["details"]] == ["body"]


def test_empty_body_is_400_and_names_the_body(client):
    r = post(client)
    assert r.status_code == 400
    assert [d["field"] for d in r.json()["details"]] == ["body"]
