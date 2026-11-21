
import argparse
from pathlib import Path

from pyflink.java_gateway import get_gateway
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import Catalog


CATALOG = "glue_catalog"
DATABASE = "lakehouse_db"
TABLE = "event_window_aggregations_test_upsert"

WAREHOUSE = "s3://coderhouse-data-flor-dev/lakehouse/"
CATALOG_REGION = "us-east-2"
S3_REGION = "us-east-1"

DEPENDENCIES_JAR = (
    Path(__file__).resolve().parents[2]
    / "target"
    / "pyflink-dependencies.jar"
)

TABLE_ID = f"{CATALOG}.{DATABASE}.{TABLE}"

# Mantener referencias Java durante toda la ejecución.
_CLASSLOADER = None
_FLINK_CATALOG = None
_CATALOG_LOADER = None


def create_table_environment():
    global _CLASSLOADER
    global _FLINK_CATALOG
    global _CATALOG_LOADER

    if not DEPENDENCIES_JAR.is_file():
        raise FileNotFoundError(DEPENDENCIES_JAR)

    table_env = TableEnvironment.create(
        EnvironmentSettings.in_batch_mode()
    )

    table_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        DEPENDENCIES_JAR.resolve().as_uri(),
    )

    gateway = get_gateway()
    jvm = gateway.jvm

    jar_url = (
        jvm.java.io.File(str(DEPENDENCIES_JAR))
        .toURI()
        .toURL()
    )

    thread = jvm.java.lang.Thread.currentThread()
    original_loader = thread.getContextClassLoader()

    url_class = jvm.java.lang.Class.forName("java.net.URL")
    urls = jvm.java.lang.reflect.Array.newInstance(
        url_class, 1
    )
    jvm.java.lang.reflect.Array.set(urls, 0, jar_url)

    loader = jvm.java.net.URLClassLoader(
        urls, original_loader
    )

    thread.setContextClassLoader(loader)

    config_class = jvm.java.lang.Class.forName(
        "org.apache.hadoop.conf.Configuration",
        True,
        loader,
    )

    hadoop_conf = config_class.newInstance()
    hadoop_conf.setClassLoader(loader)

    props = jvm.java.util.HashMap()
    props.put("warehouse", WAREHOUSE)
    props.put(
        "io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )
    props.put("client.region", CATALOG_REGION)
    props.put("s3.region", S3_REGION)
    props.put("s3.cross-region-access-enabled", "true")

    loader_class = jvm.java.lang.Class.forName(
        "org.apache.iceberg.flink.CatalogLoader",
        True,
        loader,
    )

    custom_method = next(
        method
        for method in loader_class.getMethods()
        if method.getName() == "custom"
        and method.getParameterCount() == 4
    )

    object_class = jvm.java.lang.Class.forName(
        "java.lang.Object"
    )

    loader_args = jvm.java.lang.reflect.Array.newInstance(
        object_class, 4
    )

    loader_values = [
        CATALOG,
        props,
        hadoop_conf,
        "org.apache.iceberg.aws.glue.GlueCatalog",
    ]

    for index, value in enumerate(loader_values):
        jvm.java.lang.reflect.Array.set(
            loader_args, index, value
        )

    catalog_loader = custom_method.invoke(
        None, loader_args
    )

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
        object_class, 0
    )

    namespace = empty_method.invoke(None, empty_args)

    flink_catalog_class = jvm.java.lang.Class.forName(
        "org.apache.iceberg.flink.FlinkCatalog",
        True,
        loader,
    )

    constructor = next(
        item
        for item in flink_catalog_class.getConstructors()
        if item.getParameterCount() == 7
    )

    catalog_args = jvm.java.lang.reflect.Array.newInstance(
        object_class, 7
    )

    catalog_values = [
        CATALOG,
        DATABASE,
        namespace,
        catalog_loader,
        props,
        True,
        30000,
    ]

    for index, value in enumerate(catalog_values):
        jvm.java.lang.reflect.Array.set(
            catalog_args, index, value
        )

    flink_catalog = constructor.newInstance(catalog_args)

    table_env.register_catalog(
        CATALOG,
        Catalog(flink_catalog),
    )

    _CLASSLOADER = loader
    _FLINK_CATALOG = flink_catalog
    _CATALOG_LOADER = catalog_loader

    return table_env, thread, original_loader


def ensure_test_table(table_env):
    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_ID} (
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


def write_same_record(table_env):
    insert_sql = f"""
        INSERT INTO {TABLE_ID}
        VALUES ('click', 1799990000000, 1799990060000, 2, 10.0, 5.0)
    """

    print("Primera escritura...")
    table_env.execute_sql(insert_sql).wait()

    print("Segunda escritura idéntica...")
    table_env.execute_sql(insert_sql).wait()

    print("Ambas escrituras finalizaron.")


def write_updated_record(table_env):
    insert_sql = f"""
        INSERT INTO {TABLE_ID}
        VALUES ('click', 1799990000000, 1799990060000, 5, 25.0, 5.0)
    """

    print("Escribiendo valores actualizados sobre la misma clave...")
    table_env.execute_sql(insert_sql).wait()

    print("Escritura de actualización finalizada.")



def write_distinct_windows(table_env):
    records = [
        ("click", 1800000000000, 1800000300000, 3, 30.0, 10.0),
        ("click", 1800000060000, 1800000360000, 5, 75.0, 15.0),
    ]

    for record in records:
        table_env.execute_sql(
            f"""
            INSERT INTO {TABLE_ID}
            VALUES (
                '{record[0]}',
                {record[1]},
                {record[2]},
                {record[3]},
                {record[4]},
                {record[5]}
            )
            """
        ).wait()

    print("Escritura de las dos ventanas finalizada.")


def read_table(table_env):
    result = table_env.execute_sql(
        f"""
        SELECT
            event_type,
            window_start,
            window_end,
            event_count,
            total_amount,
            average_amount
        FROM {TABLE_ID}
        """
    )

    rows = []

    with result.collect() as records:
        for row in records:
            print(row)
            rows.append(tuple(row))

    expected_rows = {
        ("click", 1799990000000, 1799990060000, 5, 25.0, 5.0),
        ("click", 1800000000000, 1800000300000, 3, 30.0, 10.0),
        ("click", 1800000060000, 1800000360000, 5, 75.0, 15.0),
    }

    assert len(rows) == 3, (
        f"Se esperaban 3 filas y se encontraron {len(rows)}"
    )

    assert len(set(rows)) == len(rows), (
        "Se encontraron filas duplicadas"
    )

    assert set(rows) == expected_rows, (
        f"Los resultados no coinciden.\n"
        f"Esperados: {expected_rows}\n"
        f"Obtenidos: {set(rows)}"
    )

    print(f"Total de filas leidas: {len(rows)}")
    print("VALIDACION EXITOSA: UPSERT y resultados correctos.")

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--action",
        choices=[
            "create",
            "write-same",
            "write-update",
            "write-distinct-windows",
            "read",
        ],
        required=True,
    )

    args = parser.parse_args()

    table_env, thread, original_loader = (
        create_table_environment()
    )

    try:
        ensure_test_table(table_env)

        if args.action == "create":
            print(
                f"Tabla de prueba creada/verificada: {TABLE_ID}"
            )

        elif args.action == "write-same":
            write_same_record(table_env)

        elif args.action == "write-update":
            write_updated_record(table_env)

        elif args.action == "write-distinct-windows":
            write_distinct_windows(table_env)

        elif args.action == "read":
            read_table(table_env)

    finally:
        thread.setContextClassLoader(original_loader)
        _CLASSLOADER.close()


if __name__ == "__main__":
    main()
