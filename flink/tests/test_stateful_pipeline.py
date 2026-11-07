import json
import sys

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment

from flink.src.stateful_processor import StatefulAggregator


def test_stateful_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()

    # Utilizar el mismo intérprete de Python del entorno virtual
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

    results.print()

    env.execute("Test-Stateful-Pipeline")


if __name__ == "__main__":
    test_stateful_pipeline()