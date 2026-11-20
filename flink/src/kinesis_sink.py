
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kinesis import (
    KinesisStreamsSink,
    PartitionKeyGenerator,
)


def create_kinesis_sink(
    stream_name: str,
    region: str = "us-east-2",
):
    """
    Construye un sink de Amazon Kinesis Data Streams.

    Los registros se reciben como strings JSON.
    """

    if not stream_name:
        raise ValueError(
            "El nombre del stream Kinesis de salida es obligatorio"
        )

    sink_properties = {
        "aws.region": region,
    }

    sink = (
        KinesisStreamsSink.builder()
        .set_stream_name(stream_name)
        .set_kinesis_client_properties(sink_properties)
        .set_serialization_schema(SimpleStringSchema())
        .set_partition_key_generator(
            PartitionKeyGenerator.random()
        )
        .set_fail_on_error(True)
        .build()
    )

    return sink
