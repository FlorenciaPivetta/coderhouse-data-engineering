
import os
import sys
from pathlib import Path

from pyflink.common import Configuration, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

from flink.config.application_config import (
    load_application_properties,
    get_application_property,
)
from flink.config.flink_config import configure_checkpoints
from flink.src.event_parser import parse_event
from flink.src.kinesis_source import create_kinesis_source
from flink.src.watermarks import create_watermark_strategy
from flink.src.window_aggregations import tumbling_window


def configure_iceberg_sink(table_env, results, application_properties):
    """
    Configura el catálogo Glue y escribe los resultados
    de las ventanas en una tabla Iceberg.
    """

    iceberg_group = "Iceberg"

    catalog_name = get_application_property(
        application_properties,
        iceberg_group,
        "catalog.name",
        "glue_catalog"
    )

    database_name = get_application_property(
        application_properties,
        iceberg_group,
        "catalog.database"
    )

    warehouse = get_application_property(
        application_properties,
        iceberg_group,
        "warehouse"
    )

    catalog_region = get_application_property(
        application_properties,
        iceberg_group,
        "catalog.region",
        "us-east-2"
    )

    s3_region = get_application_property(
        application_properties,
        iceberg_group,
        "s3.region",
        "us-east-1"
    )

    table_name = get_application_property(
        application_properties,
        iceberg_group,
        "table.name",
        "event_window_aggregations"
    )

    if not database_name or not warehouse:
        raise ValueError(
            "Faltan propiedades de Iceberg: "
            "catalog.database y/o warehouse."
        )

    # 1. Crear catálogo Iceberg utilizando AWS Glue.
    create_catalog_sql = f"""
        CREATE CATALOG {catalog_name} WITH (
            'type' = 'iceberg',
            'catalog-type' = 'glue',
            'warehouse' = '{warehouse}',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            'client.region' = '{catalog_region}',
            's3.region' = '{s3_region}'
        )
    """

    table_env.execute_sql(create_catalog_sql)

    # 2. Crear la base de datos si todavía no existe.
    table_env.execute_sql(
        f"""
        CREATE DATABASE IF NOT EXISTS
        {catalog_name}.{database_name}
        """
    )

    # 3. Crear la tabla Iceberg particionada por tipo de evento.
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS
        {catalog_name}.{database_name}.{table_name} (
            event_type STRING,
            window_start BIGINT,
            window_end BIGINT,
            event_count BIGINT,
            total_amount DOUBLE,
            average_amount DOUBLE
        )
        PARTITIONED BY (event_type)
        WITH (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
    """

    table_env.execute_sql(create_table_sql)

    # 4. Registrar los resultados del DataStream como vista temporal.
    table_env.create_temporary_view(
        "window_results",
        results,
        "event_type, window_start, window_end, "
        "event_count, total_amount, average_amount"
    )

    # 5. Insertar los resultados en Iceberg.
    insert_sql = f"""
        INSERT INTO
        {catalog_name}.{database_name}.{table_name}
        SELECT
            event_type,
            window_start,
            window_end,
            event_count,
            total_amount,
            average_amount
        FROM window_results
    """

    table_env.execute_sql(insert_sql)


def main():

    # 1. Leer propiedades de ejecución.
    application_properties = load_application_properties()

    input_group = "InputStream0"

    stream_arn = get_application_property(
        application_properties,
        input_group,
        "stream.arn"
    )

    region = get_application_property(
        application_properties,
        input_group,
        "aws.region",
        "us-east-1"
    )

    init_position = get_application_property(
        application_properties,
        input_group,
        "flink.source.init.position",
        "TRIM_HORIZON"
    )

    # 2. Configurar entorno y dependencias.
    config = Configuration()

    is_local = os.getenv(
        "IS_LOCAL",
        "false"
    ).lower() == "true"

    # Configurar intérprete Python solamente en ejecución local.
    if is_local:
        config.set_string(
            "python.executable",
            sys.executable
        )

        config.set_string(
            "python.client.executable",
            sys.executable
        )

    # Localizar el JAR de dependencias.
    script_dir = Path(__file__).resolve().parent

    jar_candidates = [
        script_dir / "flink" / "lib",
        script_dir.parent / "flink" / "lib",
        script_dir / "lib",
        Path("/opt/flink/lib"),
    ]

    dependencies_jar = next(
        (
            directory / "pyflink-dependencies.jar"
            for directory in jar_candidates
            if (directory / "pyflink-dependencies.jar").is_file()
        ),
        None
    )

    if dependencies_jar is None:
        raise FileNotFoundError(
            "No se encontró pyflink-dependencies.jar. "
            f"Ubicaciones revisadas: {jar_candidates}"
        )

    # Incorporar dependencias solamente en ejecución local.
    if is_local:
        jar_uri = dependencies_jar.resolve().as_uri()

        config.set_string(
            "pipeline.jars",
            jar_uri
        )

        config.set_string(
            "pipeline.classpaths",
            jar_uri
        )

    # 3. Crear entorno Flink.
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)

    # 4. Configurar checkpoints.
    configure_checkpoints(env)

    # 5. Seleccionar origen de datos.
    source_mode = os.getenv("FLINK_SOURCE")

    if source_mode is None:
        source_mode = "kinesis" if stream_arn else "local"

    source_mode = source_mode.lower()

    if source_mode == "kinesis":

        if not stream_arn:
            stream_arn = os.getenv("KINESIS_STREAM_ARN")

        if not stream_arn:
            raise ValueError(
                "No se encontró el ARN del stream Kinesis. "
                "Configurá stream.arn en InputStream0."
            )

        region = os.getenv("AWS_REGION", region)

        kinesis_source = create_kinesis_source(
            stream_arn=stream_arn,
            region=region,
            init_position=init_position
        )

        source = env.from_source(
            kinesis_source,
            WatermarkStrategy.no_watermarks(),
            "Amazon-Kinesis-Source",
            Types.STRING()
        )

        job_name = "Preentrega5-Flink-Kinesis"

    elif source_mode == "local":

        raw_events = [
            '{"event_id":"evt-001","event_type":"purchase","user_id":"user-123","event_time":"2026-11-02T01:00:05Z","amount":100.00}',
            '{"event_id":"evt-002","event_type":"purchase","user_id":"user-456","event_time":"2026-11-02T01:00:20Z","amount":200.00}',
            '{"event_id":"evt-003","event_type":"click","user_id":"user-123","event_time":"2026-11-02T01:00:10Z","amount":50.00}',
        ]

        source = env.from_collection(
            raw_events,
            type_info=Types.STRING()
        )

        job_name = "Preentrega5-Flink-Local"

    else:
        raise ValueError(f"Origen no reconocido: {source_mode}")

    # 6. Deserializar y validar eventos.
    parsed_stream = source.map(
        parse_event,
        output_type=Types.PICKLED_BYTE_ARRAY()
    )

    # 7. Asignar Event Time y Watermarks.
    timestamped_stream = parsed_stream.assign_timestamps_and_watermarks(
        create_watermark_strategy()
    )

    # 8. Aplicar ventana Tumbling de un minuto.
    results = tumbling_window(timestamped_stream)

    # 9. Seleccionar destino de los resultados.
    sink_mode = os.getenv(
        "FLINK_SINK",
        "iceberg" if source_mode == "kinesis" else "print"
    ).lower()

    if sink_mode == "print":

        results.print()
        env.execute(job_name)

    elif sink_mode == "iceberg":

        table_env = StreamTableEnvironment.create(env)

        configure_iceberg_sink(
            table_env,
            results,
            application_properties
        )

        # INSERT INTO envía el trabajo de streaming a Flink.
        # No ejecutar env.execute() nuevamente.

    else:
        raise ValueError(
            f"Destino no reconocido: {sink_mode}. "
            "Usá 'print' o 'iceberg'."
        )


if __name__ == "__main__":
    main()
