
from pyflink.datastream.connectors import Source
from pyflink.java_gateway import get_gateway

from flink.config.kinesis_config import create_kinesis_configuration


def create_kinesis_source(
    stream_arn: str,
    region: str = "us-east-1",
    init_position: str = "TRIM_HORIZON"
):
    """
    Construye el origen de datos de Amazon Kinesis
    utilizando el conector Java desde PyFlink.
    """

    if not stream_arn:
        raise ValueError("El ARN del stream Kinesis es obligatorio")

    gateway = get_gateway()
    jvm = gateway.jvm

    config = create_kinesis_configuration(
        region=region,
        init_position=init_position
    )

    java_config = config._j_configuration

    schema = (
        jvm.org.apache.flink.connector.kinesis.source.serialization
        .KinesisDeserializationSchema.of(
            jvm.org.apache.flink.api.common.serialization.SimpleStringSchema()
        )
    )

    java_source = (
        jvm.org.apache.flink.connector.kinesis.source.KinesisStreamsSource
        .builder()
        .setStreamArn(stream_arn)
        .setSourceConfig(java_config)
        .setDeserializationSchema(schema)
        .build()
    )

    return Source(java_source)
