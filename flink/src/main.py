
import json
import os
import sys
from pathlib import Path

from pyflink.common import Configuration, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.catalog import Catalog

from flink.config.application_config import (
    load_application_properties,
    get_application_property,
)
from flink.config.flink_config import configure_checkpoints
from flink.src.event_parser import safe_parse_event
from flink.src.kinesis_sink import create_kinesis_sink
from flink.src.kinesis_source import create_kinesis_source
from flink.src.stateful_processor import StatefulAggregator
from flink.src.watermarks import create_watermark_strategy
from flink.src.window_aggregations import tumbling_window, sliding_window


# Mantener referencias a los objetos Java durante la ejecución.
_ICEBERG_CLASSLOADER = None
_ICEBERG_FLINK_CATALOG = None
_ICEBERG_CATALOG_LOADER = None


def configure_iceberg_sink(
    table_env,
    tumbling_results,
    sliding_results,
    application_properties,
    dependencies_jar,
    statement_set,
):
    """
    Registra FlinkCatalog directamente y configura
    la escritura de resultados en una tabla Iceberg.
    """

    global _ICEBERG_CLASSLOADER
    global _ICEBERG_FLINK_CATALOG
    global _ICEBERG_CATALOG_LOADER

    iceberg_group = "Iceberg"

    catalog_name = get_application_property(
        application_properties,
        iceberg_group,
        "catalog.name",
        "glue_catalog",
    )

    database_name = get_application_property(
        application_properties,
        iceberg_group,
        "catalog.database",
    )

    warehouse = get_application_property(
        application_properties,
        iceberg_group,
        "warehouse",
    )

    catalog_region = get_application_property(
        application_properties,
        iceberg_group,
        "catalog.region",
        "us-east-2",
    )

    s3_region = get_application_property(
        application_properties,
        iceberg_group,
        "s3.region",
        "us-east-1",
    )

    table_name = get_application_property(
        application_properties,
        iceberg_group,
        "table.name",
        "event_window_aggregations",
    )

    sliding_table_name = get_application_property(
        application_properties,
        iceberg_group,
        "sliding.table.name",
        "event_sliding_window_aggregations",
    )

    if not database_name or not warehouse:
        raise ValueError(
            "Faltan propiedades de Iceberg: "
            "catalog.database y/o warehouse."
        )

    from pyflink.java_gateway import get_gateway

    g = get_gateway()
    jvm = g.jvm

    jar = Path(dependencies_jar).resolve()

    if not jar.is_file():
        raise FileNotFoundError(
            f"No existe el JAR de dependencias: {jar}"
        )

    # Crear un classloader con el JAR de dependencias.
    url = jvm.java.io.File(str(jar)).toURI().toURL()

    parent = (
        jvm.java.lang.Thread.currentThread()
        .getContextClassLoader()
    )

    url_class = jvm.java.lang.Class.forName("java.net.URL")

    urls = jvm.java.lang.reflect.Array.newInstance(
        url_class,
        1,
    )

    jvm.java.lang.reflect.Array.set(urls, 0, url)

    loader = jvm.java.net.URLClassLoader(urls, parent)

    thread = jvm.java.lang.Thread.currentThread()
    original_loader = thread.getContextClassLoader()

    try:
        thread.setContextClassLoader(loader)

        # Crear configuración Hadoop.
        config_class = jvm.java.lang.Class.forName(
            "org.apache.hadoop.conf.Configuration",
            True,
            loader,
        )

        conf = config_class.newInstance()
        conf.setClassLoader(loader)

        # Configurar propiedades de Iceberg.
        props = jvm.java.util.HashMap()

        props.put("warehouse", warehouse)

        props.put(
            "io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )

        props.put("client.region", catalog_region)
        props.put("s3.region", s3_region)
        props.put("s3.cross-region-access-enabled", "true")

        # Crear CatalogLoader para AWS Glue.
        lc = jvm.java.lang.Class.forName(
            "org.apache.iceberg.flink.CatalogLoader",
            True,
            loader,
        )

        loader_method = next(
            method
            for method in lc.getMethods()
            if method.getName() == "custom"
            and method.getParameterCount() == 4
        )

        obj = jvm.java.lang.Class.forName(
            "java.lang.Object"
        )

        args = jvm.java.lang.reflect.Array.newInstance(
            obj,
            4,
        )

        values = [
            catalog_name,
            props,
            conf,
            "org.apache.iceberg.aws.glue.GlueCatalog",
        ]

        for i, value in enumerate(values):
            jvm.java.lang.reflect.Array.set(
                args,
                i,
                value,
            )

        catalog_loader = loader_method.invoke(None, args)

        # Obtener Namespace.empty().
        namespace_class = jvm.java.lang.Class.forName(
            "org.apache.iceberg.catalog.Namespace",
            True,
            loader,
        )

        empty_method = next(
            method
            for method in namespace_class.getMethods()
            if method.getName() == "empty"
            and method.getParameterCount() == 0
        )

        empty_args = jvm.java.lang.reflect.Array.newInstance(
            obj,
            0,
        )

        namespace = empty_method.invoke(
            None,
            empty_args,
        )

        # Construir FlinkCatalog.
        flink_catalog_class = jvm.java.lang.Class.forName(
            "org.apache.iceberg.flink.FlinkCatalog",
            True,
            loader,
        )

        constructor = next(
            c
            for c in flink_catalog_class.getConstructors()
            if c.getParameterCount() == 7
        )

        constructor_args = jvm.java.lang.reflect.Array.newInstance(
            obj,
            7,
        )

        values = [
            catalog_name,
            database_name,
            namespace,
            catalog_loader,
            props,
            True,
            30000,
        ]

        for i, value in enumerate(values):
            jvm.java.lang.reflect.Array.set(
                constructor_args,
                i,
                value,
            )

        flink_catalog = constructor.newInstance(
            constructor_args
        )

        # Registrar el catálogo en PyFlink.
        table_env.register_catalog(
            catalog_name,
            Catalog(flink_catalog),
        )

        # Mantener referencias durante la ejecución.
        _ICEBERG_CLASSLOADER = loader
        _ICEBERG_FLINK_CATALOG = flink_catalog
        _ICEBERG_CATALOG_LOADER = catalog_loader

        print(
            "Catálogos disponibles:",
            table_env.list_catalogs(),
        )

        # La base de datos ya existe en AWS Glue.
        print(
            f"Base de datos Glue existente: {database_name}"
        )

        # Crear tablas nuevas para UPSERT sin modificar las tablas originales.
        # La clave lógica identifica una ventana por tipo y límites temporales.
        def upsert_table_name(base_name):
            suffix = "_upsert"
            return (
                base_name
                if base_name.endswith(suffix)
                else f"{base_name}{suffix}"
            )

        window_tables = [
            (
                upsert_table_name(table_name),
                tumbling_results,
                "tumbling_window_results_upsert",
                "Tumbling",
                table_name,
            ),
            (
                upsert_table_name(sliding_table_name),
                sliding_results,
                "sliding_window_results_upsert",
                "Sliding",
                sliding_table_name,
            ),
        ]

        for (
            target_table,
            window_results,
            view_name,
            window_label,
            original_table,
        ) in window_tables:

            table_env.execute_sql(
                f"""
                CREATE TABLE IF NOT EXISTS
                {catalog_name}.{database_name}.{target_table} (
                    event_type STRING NOT NULL,
                    window_start BIGINT NOT NULL,
                    window_end BIGINT NOT NULL,
                    event_count BIGINT,
                    total_amount DOUBLE,
                    average_amount DOUBLE,
                    PRIMARY KEY (
                        event_type,
                        window_start,
                        window_end
                    ) NOT ENFORCED
                )
                PARTITIONED BY (event_type)
                WITH (
                    'format-version' = '2',
                    'write.format.default' = 'parquet',
                    'write.upsert.enabled' = 'true'
                )
                """
            )

            # Convertir los resultados de esta ventana en una Table.
            results_table = table_env.from_data_stream(window_results)

            # Registrar una vista temporal independiente.
            table_env.create_temporary_view(
                view_name,
                results_table,
            )

            # Incorporar la escritura al mismo StatementSet.
            statement_set.add_insert_sql(
                f"""
                INSERT INTO
                {catalog_name}.{database_name}.{target_table}
                SELECT
                    event_type,
                    window_start,
                    window_end,
                    event_count,
                    total_amount,
                    average_amount
                FROM {view_name}
                """
            )

            print(
                f"Tabla Iceberg {window_label} configurada para UPSERT: "
                f"{catalog_name}.{database_name}.{target_table} "
                f"(tabla anterior conservada: {original_table})"
            )

        print(
            "Catálogo Iceberg y tablas Tumbling/Sliding configurados."
        )

    finally:
        # Restaurar el classloader original.
        # No cerrar el loader: se mantiene disponible
        # durante la ejecución del trabajo.
        thread.setContextClassLoader(original_loader)


def main():

    # -----------------------------------------------------
    # 1. Leer propiedades de ejecución.
    # -----------------------------------------------------

    application_properties = load_application_properties()

    # Configuración del stream de entrada.
    input_group = "InputStream0"

    stream_arn = get_application_property(
        application_properties,
        input_group,
        "stream.arn",
    )

    region = get_application_property(
        application_properties,
        input_group,
        "aws.region",
        "us-east-1",
    )

    init_position = get_application_property(
        application_properties,
        input_group,
        "flink.source.init.position",
        "TRIM_HORIZON",
    )

    # Configuración del stream de salida.
    output_group = "OutputStream0"

    output_stream_name = get_application_property(
        application_properties,
        output_group,
        "stream.name",
    )

    output_region = get_application_property(
        application_properties,
        output_group,
        "aws.region",
        region,
    )

    # -----------------------------------------------------
    # 2. Configurar entorno y dependencias.
    # -----------------------------------------------------

    config = Configuration()

    is_local = os.getenv(
        "IS_LOCAL",
        "false",
    ).lower() == "true"

    if is_local:

        config.set_string(
            "python.executable",
            sys.executable,
        )

        config.set_string(
            "python.client.executable",
            sys.executable,
        )

    # -----------------------------------------------------
    # 3. Localizar el JAR de dependencias.
    # -----------------------------------------------------

    script_dir = Path(__file__).resolve().parent

    jar_candidates = [
        # Estructura del ZIP desplegado en Managed Flink:
        # main.py está en la raíz y las dependencias en flink/lib/.
        script_dir / "flink" / "lib",

        # flink/lib/ cuando main.py se ejecuta desde flink/src/
        script_dir.parent / "lib",

        # target/ del proyecto
        script_dir.parent.parent / "target",

        # lib/ junto a main.py
        script_dir / "lib",

        # Directorio de Flink en AWS
        Path("/opt/flink/lib"),
    ]

    dependencies_jar = next(
        (
            directory / "pyflink-dependencies.jar"
            for directory in jar_candidates
            if (
                directory / "pyflink-dependencies.jar"
            ).is_file()
        ),
        None,
    )

    if dependencies_jar is None:
        raise FileNotFoundError(
            "No se encontró pyflink-dependencies.jar. "
            f"Ubicaciones revisadas: {jar_candidates}"
        )

    print(
        f"JAR de dependencias encontrado: {dependencies_jar}"
    )

    jar_uri = dependencies_jar.resolve().as_uri()

    # En Managed Flink, el JAR se carga mediante jarfile.
    # Solo en ejecución local se agrega explícitamente.
    if is_local:

        config.set_string(
            "pipeline.jars",
            jar_uri,
        )

    # -----------------------------------------------------
    # 4. Crear entorno Flink.
    # -----------------------------------------------------

    env = StreamExecutionEnvironment.get_execution_environment(
        config
    )

    env.set_parallelism(1)

    # -----------------------------------------------------
    # 5. Configurar checkpoints.
    # -----------------------------------------------------

    configure_checkpoints(env)

    # -----------------------------------------------------
    # 6. Seleccionar origen de datos.
    # -----------------------------------------------------

    source_mode = os.getenv("FLINK_SOURCE")

    if source_mode is None:

        source_mode = (
            "kinesis" if stream_arn else "local"
        )

    source_mode = source_mode.lower()

    if source_mode == "kinesis":

        if not stream_arn:

            stream_arn = os.getenv(
                "KINESIS_STREAM_ARN"
            )

        if not stream_arn:
            raise ValueError(
                "No se encontró el ARN del stream Kinesis. "
                "Configurá stream.arn en InputStream0."
            )

        region = os.getenv(
            "AWS_REGION",
            region,
        )

        kinesis_source = create_kinesis_source(
            stream_arn=stream_arn,
            region=region,
            init_position=init_position,
        )

        source = env.from_source(
            kinesis_source,
            WatermarkStrategy.no_watermarks(),
            "Amazon-Kinesis-Source",
            Types.STRING(),
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
            type_info=Types.STRING(),
        )

        job_name = "Preentrega5-Flink-Local"

    else:

        raise ValueError(
            f"Origen no reconocido: {source_mode}"
        )

    # -----------------------------------------------------
    # 7. Deserializar y validar eventos.
    # -----------------------------------------------------

    parsed_stream = source.map(
        safe_parse_event,
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    # -----------------------------------------------------
    # 8. Descartar eventos inválidos.
    # -----------------------------------------------------

    valid_stream = parsed_stream.filter(
        lambda event: event is not None
    )

    # -----------------------------------------------------
    # 9. Rama original de Event Time y Watermarks.
    # -----------------------------------------------------

    timestamped_stream = (
        valid_stream.assign_timestamps_and_watermarks(
            create_watermark_strategy()
        )
    )

    # -----------------------------------------------------
    # 10. Aplicar ventana Tumbling de un minuto.
    # -----------------------------------------------------

    tumbling_results = tumbling_window(timestamped_stream)
    sliding_results = sliding_window(timestamped_stream)

    # -----------------------------------------------------
    # 11. Rama adicional de procesamiento con estado.
    # -----------------------------------------------------

    # Acumula eventos por event_type utilizando ValueState
    # y TTL de 24 horas basado en Processing Time.

    stateful_results = (
        valid_stream
        .key_by(lambda event: event["event_type"])
        .process(
            StatefulAggregator(),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
    )

    # -----------------------------------------------------
    # 12. Seleccionar destino.
    # -----------------------------------------------------

    sink_mode = os.getenv(
        "FLINK_SINK",
        "iceberg" if source_mode == "kinesis" else "print",
    ).lower()

    # -----------------------------------------------------
    # MODO PRINT: pruebas locales.
    # -----------------------------------------------------

    if sink_mode == "print":

        stateful_results.print(
            "Stateful Aggregation Results"
        )

        tumbling_results.print(
            "Tumbling Window Results"
        )

        sliding_results.print(
            "Sliding Window Results"
        )

        env.execute(job_name)

    # -----------------------------------------------------
    # MODO ICEBERG: procesamiento en AWS.
    # -----------------------------------------------------

    elif sink_mode == "iceberg":

        # Rama 1: resultados stateful hacia Kinesis.
        if output_stream_name:

            # Convertir los diccionarios a JSON.
            stateful_json = stateful_results.map(
                lambda result: json.dumps(
                    result,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                output_type=Types.STRING(),
            )

            # Construir el sink de Kinesis.
            kinesis_sink = create_kinesis_sink(
                stream_name=output_stream_name,
                region=output_region,
            )

            # Conectar la salida stateful con Kinesis.
            stateful_json.sink_to(
                kinesis_sink
            ).name(
                "Stateful-Kinesis-Sink"
            )

            print(
                f"Stateful sink configurado: "
                f"{output_stream_name} ({output_region})"
            )

        else:

            # Compatibilidad mientras no exista OutputStream0.
            stateful_results.print(
                "Stateful Aggregation Results"
            )

            print(
                "ADVERTENCIA: OutputStream0 no configurado. "
                "Los resultados stateful solo se imprimen."
            )

        # Rama 2: resultados de ventanas hacia Iceberg.
        table_env = StreamTableEnvironment.create(env)
        statement_set = table_env.create_statement_set()

        configure_iceberg_sink(
            table_env,
            tumbling_results,
            sliding_results,
            application_properties,
            dependencies_jar,
            statement_set,
        )


        # Mantener el classloader de Iceberg durante la planificación
        # y ejecución del trabajo.
        from pyflink.java_gateway import get_gateway

        jvm = get_gateway().jvm
        thread = jvm.java.lang.Thread.currentThread()
        original_loader = thread.getContextClassLoader()

        if _ICEBERG_CLASSLOADER is None:
            raise RuntimeError(
                "No se inicializó el classloader de Iceberg."
            )

        try:
            thread.setContextClassLoader(_ICEBERG_CLASSLOADER)

            # Integrar Table API al entorno DataStream.
            statement_set.attach_as_datastream()

            print(
                "Ramas Kinesis e Iceberg integradas en el mismo entorno Flink."
            )

            # Ejecutar ambas ramas en un único trabajo.
            env.execute(job_name)

        finally:
            thread.setContextClassLoader(original_loader)


    else:

        raise ValueError(
            f"Destino no reconocido: {sink_mode}. "
            "Usá 'print' o 'iceberg'."
        )


if __name__ == "__main__":
    main()
