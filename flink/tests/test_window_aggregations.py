
from types import SimpleNamespace

from flink.src.window_aggregations import (
    EventWindowAggregator,
    EventWindowResult,
    tumbling_window,
    sliding_window,
)
from flink.src.watermarks import EventTimestampAssigner, create_watermark_strategy


def test_event_window_aggregator():
    aggregator = EventWindowAggregator()

    accumulator = aggregator.create_accumulator()

    accumulator = aggregator.add(
        {"amount": 100.0}, accumulator
    )
    accumulator = aggregator.add(
        {"amount": 200.0}, accumulator
    )

    result = aggregator.get_result(accumulator)

    assert result["event_count"] == 2
    assert result["total_amount"] == 300.0
    assert result["average_amount"] == 150.0


def test_event_window_aggregator_empty():
    aggregator = EventWindowAggregator()

    result = aggregator.get_result(
        aggregator.create_accumulator()
    )

    assert result["event_count"] == 0
    assert result["total_amount"] == 0.0
    assert result["average_amount"] == 0.0


def test_event_window_aggregator_merge():
    aggregator = EventWindowAggregator()

    accumulator_a = {
        "event_count": 2,
        "total_amount": 300.0,
    }

    accumulator_b = {
        "event_count": 3,
        "total_amount": 150.0,
    }

    merged = aggregator.merge(accumulator_a, accumulator_b)

    assert merged["event_count"] == 5
    assert merged["total_amount"] == 450.0


def test_event_window_result():
    window_result = EventWindowResult()

    window = SimpleNamespace(
        start=1000,
        end=61000,
    )

    aggregate = {
        "event_count": 2,
        "total_amount": 300.0,
        "average_amount": 150.0,
    }

    results = list(
        window_result.apply(
            "purchase",
            window,
            [aggregate],
        )
    )

    assert len(results) == 1

    result = results[0]

    assert result.event_type == "purchase"
    assert result.window_start == 1000
    assert result.window_end == 61000
    assert result.event_count == 2
    assert result.total_amount == 300.0
    assert result.average_amount == 150.0


def test_event_timestamp_assigner():
    assigner = EventTimestampAssigner()

    event = {
        "event_time": 1793581205000
    }

    timestamp = assigner.extract_timestamp(event, 0)

    assert timestamp == 1793581205000


def test_watermark_strategy_creation():
    strategy = create_watermark_strategy()

    assert strategy is not None


def test_tumbling_window_configuration(monkeypatch):
    import flink.src.window_aggregations as windows

    captured = {}

    class FakeKeyedStream:
        def window(self, assigner):
            captured["assigner"] = assigner
            return self

        def aggregate(self, *args, **kwargs):
            captured["aggregate"] = args
            captured["kwargs"] = kwargs
            return "tumbling-result"

    class FakeStream:
        def key_by(self, selector):
            captured["selector"] = selector
            return FakeKeyedStream()

    result = windows.tumbling_window(FakeStream())

    assert result == "tumbling-result"
    assert isinstance(
        captured["assigner"],
        windows.TumblingEventTimeWindows,
    )
    assert callable(captured["selector"])


def test_sliding_window_configuration():
    import flink.src.window_aggregations as windows

    captured = {}

    class FakeKeyedStream:
        def window(self, assigner):
            captured["assigner"] = assigner
            return self

        def aggregate(self, *args, **kwargs):
            return "sliding-result"

    class FakeStream:
        def key_by(self, selector):
            return FakeKeyedStream()

    result = windows.sliding_window(FakeStream())

    assert result == "sliding-result"
    assert isinstance(
        captured["assigner"],
        windows.SlidingEventTimeWindows,
    )


def test_tumbling_window_exact_duration(monkeypatch):
    import flink.src.window_aggregations as windows

    captured = {}

    class FakeTime:
        @staticmethod
        def minutes(value):
            captured["minutes"] = value
            return ("minutes", value)

    class FakeTumbling:
        @staticmethod
        def of(size):
            captured["size"] = size
            return "tumbling-assigner"

    class FakeKeyedStream:
        def window(self, assigner):
            captured["assigner"] = assigner
            return self

        def aggregate(self, *args, **kwargs):
            return "result"

    class FakeStream:
        def key_by(self, selector):
            return FakeKeyedStream()

    monkeypatch.setattr(windows, "Time", FakeTime)
    monkeypatch.setattr(
        windows, "TumblingEventTimeWindows", FakeTumbling
    )

    result = windows.tumbling_window(FakeStream())

    assert result == "result"
    assert captured["minutes"] == 1
    assert captured["size"] == ("minutes", 1)
    assert captured["assigner"] == "tumbling-assigner"


def test_sliding_window_exact_duration_and_slide(monkeypatch):
    import flink.src.window_aggregations as windows

    captured = {}

    class FakeTime:
        @staticmethod
        def minutes(value):
            return ("minutes", value)

    class FakeSliding:
        @staticmethod
        def of(size, slide):
            captured["size"] = size
            captured["slide"] = slide
            return "sliding-assigner"

    class FakeKeyedStream:
        def window(self, assigner):
            captured["assigner"] = assigner
            return self

        def aggregate(self, *args, **kwargs):
            return "result"

    class FakeStream:
        def key_by(self, selector):
            return FakeKeyedStream()

    monkeypatch.setattr(windows, "Time", FakeTime)
    monkeypatch.setattr(
        windows, "SlidingEventTimeWindows", FakeSliding
    )

    result = windows.sliding_window(FakeStream())

    assert result == "result"
    assert captured["size"] == ("minutes", 5)
    assert captured["slide"] == ("minutes", 1)
    assert captured["assigner"] == "sliding-assigner"


def test_watermark_exact_out_of_orderness(monkeypatch):
    import flink.src.watermarks as watermarks

    captured = {}

    class FakeDuration:
        @staticmethod
        def of_seconds(value):
            captured["seconds"] = value
            return ("seconds", value)

    class FakeStrategy:
        @staticmethod
        def for_bounded_out_of_orderness(duration):
            captured["duration"] = duration

            class Strategy:
                def with_timestamp_assigner(self, assigner):
                    captured["assigner"] = assigner
                    return "watermark-strategy"

            return Strategy()

    monkeypatch.setattr(watermarks, "Duration", FakeDuration)
    monkeypatch.setattr(
        watermarks, "WatermarkStrategy", FakeStrategy
    )

    strategy = watermarks.create_watermark_strategy()

    assert strategy == "watermark-strategy"
    assert captured["seconds"] == 30
    assert captured["duration"] == ("seconds", 30)
    assert isinstance(
        captured["assigner"],
        watermarks.EventTimestampAssigner,
    )
