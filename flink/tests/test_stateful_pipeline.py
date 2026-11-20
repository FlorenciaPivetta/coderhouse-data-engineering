
import json
import sys

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment

from flink.src.stateful_processor import StatefulAggregator
from flink.src.event_parser import safe_parse_event


def test_stateful_pipeline():
    """
    Verifica el procesamiento acumulativo por tipo de evento.
    """

    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_python_executable(sys.executable)
    env.set_parallelism(1)

    raw_events = [
        '{"event_type":"purchase","amount":100.0}',
        '{"event_type":"purchase","amount":200.0}',
        '{"event_type":"click","amount":50.0}',
    ]

    source = env.from_collection(
        raw_events,
        type_info=Types.STRING()
    )

    parsed_stream = source.map(
        lambda event: json.loads(event),
        output_type=Types.PICKLED_BYTE_ARRAY()
    )

    results = (
        parsed_stream
        .key_by(lambda event: event["event_type"])
        .process(StatefulAggregator())
    )

    iterator = results.execute_and_collect(
        "Test-Stateful-Pipeline"
    )

    try:
        output = list(iterator)
    finally:
        iterator.close()

    assert len(output) == 3

    purchase_results = [
        event for event in output
        if event["event_type"] == "purchase"
    ]

    click_results = [
        event for event in output
        if event["event_type"] == "click"
    ]

    assert len(purchase_results) == 2
    assert len(click_results) == 1

    purchase_results.sort(
        key=lambda event: event["event_count"]
    )

    assert purchase_results[0]["event_count"] == 1
    assert purchase_results[0]["total_amount"] == 100.0

    assert purchase_results[1]["event_count"] == 2
    assert purchase_results[1]["total_amount"] == 300.0
    assert purchase_results[1]["average_amount"] == 150.0

    assert click_results[0]["event_count"] == 1
    assert click_results[0]["total_amount"] == 50.0
    assert click_results[0]["average_amount"] == 50.0


def test_safe_parser_filter_pipeline():
    """
    Verifica que PyFlink procese eventos válidos
    y descarte los inválidos sin interrumpir el pipeline.
    """

    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_python_executable(sys.executable)
    env.set_parallelism(1)

    raw_events = [
        # Evento válido.
        '{"event_id":"evt-001","event_type":"purchase",'
        '"user_id":"user-123","event_time":"2026-11-11T10:00:00Z",'
        '"amount":100.0}',

        # Evento inválido: falta user_id.
        '{"event_id":"evt-002","event_type":"click",'
        '"event_time":"2026-11-11T10:00:10Z","amount":50.0}',

        # JSON inválido.
        '{"event_id": "evt-003", json_invalido}',

        # Evento inválido: importe negativo.
        '{"event_id":"evt-004","event_type":"refund",'
        '"user_id":"user-456","event_time":"2026-11-11T10:00:20Z",'
        '"amount":-30.0}',
    ]

    source = env.from_collection(
        raw_events,
        type_info=Types.STRING()
    )

    parsed_stream = source.map(
        safe_parse_event,
        output_type=Types.PICKLED_BYTE_ARRAY()
    )

    valid_stream = parsed_stream.filter(
        lambda event: event is not None
    )

    iterator = valid_stream.execute_and_collect(
        "Test-Safe-Parser-Filter-Pipeline"
    )

    try:
        output = list(iterator)
    finally:
        iterator.close()

    # Solo debe llegar el evento válido.
    assert len(output) == 1

    event = output[0]

    assert event["event_id"] == "evt-001"
    assert event["event_type"] == "purchase"
    assert event["user_id"] == "user-123"
    assert event["amount"] == 100.0
    assert event["event_time"] > 0


if __name__ == "__main__":
    test_stateful_pipeline()
    test_safe_parser_filter_pipeline()
