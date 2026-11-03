
from flink.src.stateful_processor import StatefulAggregator


def test_stateful_aggregator_processes_events():
    aggregator = StatefulAggregator()

    class FakeState:
        def __init__(self):
            self.current_value = None

        def value(self):
            return self.current_value

        def update(self, value):
            self.current_value = value

    class FakeRuntimeContext:
        def get_state(self, descriptor):
            if descriptor.name == "event_count":
                return count_state
            if descriptor.name == "total_amount":
                return amount_state

    count_state = FakeState()
    amount_state = FakeState()

    aggregator.open(FakeRuntimeContext())

    first_result = next(aggregator.process_element(
        {"event_type": "purchase", "amount": 100.0},
        None
    ))

    second_result = next(aggregator.process_element(
        {"event_type": "purchase", "amount": 200.0},
        None
    ))

    assert first_result["event_count"] == 1
    assert first_result["total_amount"] == 100.0

    assert second_result["event_count"] == 2
    assert second_result["total_amount"] == 300.0
    assert second_result["average_amount"] == 150.0
