# SQL recuperado — Checkpoint 6

Fuente: `CHECKPOINT 6 – Analítica avanzada in-stream con Amazon Redshift` (16 páginas).

## Trazabilidad

- **EJECUTADA Y VALIDADA**: sentencia y comportamiento descritos como comprobados en el Checkpoint 6.
- **RECONSTRUIDA**: el PDF indica que el SQL original no pudo recuperarse; la sentencia se reconstruyó a partir de columnas/resultados observados.
- **PLANTILLA**: requiere verificar/completar valores del entorno antes de ejecutarse.
- **CONSULTA DE VALIDACIÓN**: lectura/monitoreo; no crea ni modifica objetos.

## Archivos

1. `01_external_schema_glue.sql` — plantilla para el esquema externo `lakehouse`. Verificar ARN y permisos antes de usar.
2. `02_redshift_streaming_ingestion.sql` — MV `mv_kinesis_raw` documentada como ejecutada y validada; incluye por separado `mv_kinesis_events`, que el PDF identifica como reconstruida.
3. `03_kinesis_iceberg_integration.sql` — vista intermedia y vista de consumo; reproduce la nomenclatura del anexo SQL.
4. `04_analytics_security.sql` — esquema, rol, vista, grants y usuario de prueba descritos en el PDF.
5. `05_monitoring_validation.sql` — consultas de monitoreo y validación documentadas.

## Advertencias antes de cualquier ejecución

- No ejecutar los archivos como un único script sin revisar el estado real de Redshift.
- No ejecutar `CREATE USER`/`CREATE ROLE` ni grants en producción sin revisar si ya existen y contar con autorización.
- `CREATE MATERIALIZED VIEW public.mv_kinesis_events` es una reconstrucción documentada, no el DDL original recuperado.
- `CREATE EXTERNAL SCHEMA lakehouse` es una plantilla: reemplazar el marcador del ARN únicamente después de verificar el rol real y la región.
- El PDF tiene una diferencia de nomenclatura: en el cuerpo describe `analytics.vw_kinesis_iceberg`; en el anexo crea primero `public.vw_analytics_kinesis_iceberg` y luego `analytics.vw_kinesis_iceberg`. Se conserva esa secuencia del anexo.
- El Checkpoint 6 consultó `lakehouse.event_window_aggregations`. No incluye DDL de creación de las tablas Iceberg, porque esas tablas eran creadas por el pipeline Flink/Glue.
- Estos scripts reflejan el Checkpoint 6. No reemplazan la auditoría posterior del DAAT sobre `public.mv_flink_stateful_output_v2`, que es otra ruta de datos.

## Alcance

Estos archivos recuperan y ordenan el SQL incluido en el documento entregado. No certifican que todos los objetos sigan existiendo ni que los scripts sean directamente reejecutables en el estado actual de AWS.
