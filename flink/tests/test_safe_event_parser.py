
import json

from flink.src.event_parser import safe_parse_event


def valid_event():
    return json.dumps({
        "event_id": "test-001",
        "event_type": "purchase",
        "user_id": "user-123",
        "event_time": "2026-11-11T10:00:00Z",
        "amount": 100.0
    })


def test_safe_parse_valid_event():
    result = safe_parse_event(valid_event())

    assert result is not None
    assert result["event_id"] == "test-001"
    assert result["event_type"] == "purchase"
    assert result["user_id"] == "user-123"
    assert result["amount"] == 100.0


def test_safe_parse_missing_user_id():
    event = {
        "event_id": "test-002",
        "event_type": "click",
        "event_time": "2026-11-11T10:00:00Z",
        "amount": 50.0
    }

    result = safe_parse_event(json.dumps(event))

    assert result is None


def test_safe_parse_invalid_json():
    result = safe_parse_event("{json invalido")

    assert result is None


def test_safe_parse_negative_amount():
    event = json.loads(valid_event())
    event["amount"] = -100

    result = safe_parse_event(json.dumps(event))

    assert result is None


def test_safe_parse_null_event_time():
    event = json.loads(valid_event())
    event["event_time"] = None

    assert safe_parse_event(json.dumps(event)) is None


def test_safe_parse_numeric_event_time():
    event = json.loads(valid_event())
    event["event_time"] = 123456789

    assert safe_parse_event(json.dumps(event)) is None


def test_safe_parse_nan_amount():
    event = json.loads(valid_event())
    event["amount"] = float("nan")

    assert safe_parse_event(json.dumps(event)) is None


def test_safe_parse_infinite_amount():
    event = json.loads(valid_event())
    event["amount"] = float("inf")

    assert safe_parse_event(json.dumps(event)) is None


def test_safe_parse_json_array():
    assert safe_parse_event("[]") is None
