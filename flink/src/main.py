
import os
from pathlib import Path

from pyflink.common import Configuration, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment

from flink.config.application_config import (
    load_application_properties,
    get_application_property,
)
from flink.config.flink_config import configure_checkpoints
from flink.src.event_parser import parse_event
from flink.src.kinesis_source import create_kinesis_source
from flink.src.watermarks import create_watermark_strategy
from flink.src.window_aggregations import tumbling_window


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


         # 2. Configurar JAR de dependencias solo para ejecución local.
    config = Configuration()

    is_local = os.getenv("IS_LOCAL", "false").lower() == "true"

    if is_local:
        script_dir = Path(__file__).resolve().parent

        jar_candidates = [
            script_dir / "flink" / "lib",
            script_dir.parent.parent / "flink" / "lib",
            script_dir / "lib",
            script_dir.parent.parent / "target",
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

        config.set_string(
            "pipeline.jars",
            dependencies_jar.resolve().as_uri()
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

        job_name = "Preentrega4-Flink-Kinesis"

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

        job_name = "Preentrega4-Flink-Local"

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

    # 9. Mostrar resultados.
    results.print()

    # 10. Ejecutar pipeline.
    env.execute(job_name)


if __name__ == "__main__":
    main()
