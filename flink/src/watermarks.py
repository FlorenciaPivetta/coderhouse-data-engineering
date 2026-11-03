
from pyflink.common import Duration, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner


class EventTimestampAssigner(TimestampAssigner):
    """
    Extrae el timestamp del momento en que ocurrió el evento.
    """

    def extract_timestamp(self, event, record_timestamp):
        return int(event["event_time"])


def create_watermark_strategy():
    """
    Permite hasta 30 segundos de desorden
    en la llegada de los eventos.
    """

    return (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(30))
        .with_timestamp_assigner(EventTimestampAssigner())
    )
