
import pytest

from flink.src.event_parser import parse_event


def test_parse_valid_event():
    raw_event = (
        '{"event_id":"evt-001",'
        '"event_type":"purchase",'
        '"user_id":"user-123",'
        '"event_time":"2026-11-02T01:00:05Z",'
        '"amount":100.00}'
    )

    event = parse_event(raw_event)

    assert event["event_id"] == "evt-001"
    assert event["event_type"] == "purchase"
    assert event["user_id"] == "user-123"
    assert event["amount"] == 100.0
    assert event["event_time"] > 0


def test_parse_negative_amount():
    raw_event = (
        '{"event_id":"evt-002",'
        '"event_type":"purchase",'
        '"user_id":"user-123",'
        '"event_time":"2026-11-02T01:00:05Z",'
        '"amount":-100}'
    )

    with pytest.raises(ValueError):
        parse_event(raw_event)


def test_parse_missing_field():
    raw_event = (
        '{"event_id":"evt-003",'
        '"event_type":"purchase"}'
    )

    with pytest.raises(ValueError):
        parse_event(raw_event)
