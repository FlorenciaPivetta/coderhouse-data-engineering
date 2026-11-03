from pyflink.common import Time
from pyflink.datastream import Window
from pyflink.datastream.window import (
    TumblingEventTimeWindows,
    SlidingEventTimeWindows,
)
from pyflink.datastream.functions import AggregateFunction


class EventWindowAggregator(AggregateFunction):

    def create_accumulator(self):
        return {
            "event_count": 0,
            "total_amount": 0.0,
        }

    def add(self, event, accumulator):
        accumulator["event_count"] += 1
        accumulator["total_amount"] += float(event["amount"])
        return accumulator

    def get_result(self, accumulator):
        count = accumulator["event_count"]
        total = accumulator["total_amount"]

        return {
            "event_count": count,
            "total_amount": round(total, 2),
            "average_amount": round(total / count, 2) if count else 0.0,
        }

    def merge(self, accumulator_a, accumulator_b):
        return {
            "event_count": (
                accumulator_a["event_count"] + accumulator_b["event_count"]
            ),
            "total_amount": (
                accumulator_a["total_amount"] + accumulator_b["total_amount"]
            ),
        }


def tumbling_window(stream):
    return (
        stream
        .key_by(lambda event: event["event_type"])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .aggregate(EventWindowAggregator())
    )


def sliding_window(stream):
    return (
        stream
        .key_by(lambda event: event["event_type"])
        .window(SlidingEventTimeWindows.of(
            Time.minutes(5),
            Time.minutes(1)
        ))
        .aggregate(EventWindowAggregator())
    )