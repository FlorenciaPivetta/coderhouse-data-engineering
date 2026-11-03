from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types


class StatefulAggregator(KeyedProcessFunction):

    def open(self, runtime_context):
        count_descriptor = ValueStateDescriptor(
            "event_count",
            Types.LONG()
        )

        amount_descriptor = ValueStateDescriptor(
            "total_amount",
            Types.DOUBLE()
        )

        self.event_count = runtime_context.get_state(count_descriptor)
        self.total_amount = runtime_context.get_state(amount_descriptor)

    def process_element(self, event, ctx):
        count = self.event_count.value()
        total = self.total_amount.value()

        count = 0 if count is None else count
        total = 0.0 if total is None else total

        count += 1
        total += event["amount"]

        self.event_count.update(count)
        self.total_amount.update(total)

        yield {
            "event_type": event["event_type"],
            "event_count": count,
            "total_amount": round(total, 2),
            "average_amount": round(total / count, 2)
        }