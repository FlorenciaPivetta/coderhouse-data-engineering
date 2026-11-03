
from pyflink.common import Configuration


def create_kinesis_configuration(
    region="us-east-1",
    init_position="TRIM_HORIZON"
):
    """
    Configura el consumidor de Amazon Kinesis.
    """

    config = Configuration()

    # Región de AWS.
    config.set_string("aws.region", region)

    # Posición inicial de lectura.
    config.set_string(
        "source.init.position",
        init_position.upper()
    )

    # Tipo de lector.
    config.set_string("source.reader.type", "POLLING")

    # Descubrimiento de nuevos shards.
    config.set_string(
        "source.shard.discovery.interval",
        "60 s"
    )

    # Máximo de registros por consulta.
    config.set_integer(
        "source.shard.get-records.max-record-count",
        1000
    )

    return config
