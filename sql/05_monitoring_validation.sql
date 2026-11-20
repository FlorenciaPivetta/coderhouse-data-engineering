/*
CHECKPOINT 6 — REFRESH, MONITOREO Y VALIDACIÓN

Consultas recuperadas del anexo SQL del Checkpoint 6.
Son consultas de lectura, excepto el REFRESH manual indicado explícitamente.
*/

/* REFRESH MANUAL — el PDF indica que fue ejecutado. */
-- REFRESH MATERIALIZED VIEW public.mv_kinesis_raw;


/* MONITOREO DE LECTURAS DEL STREAM — consulta ejecutada y validada. */
SELECT
    mv_name,
    external_schema_name,
    stream_name,
    MAX(record_time) AS ultima_lectura,
    SUM(scanned_rows) AS registros_leidos,
    SUM(skipped_rows) AS registros_omitidos,
    SUM(scanned_bytes) AS bytes_leidos,
    MAX(stream_record_time_max) AS ultima_llegada_kinesis
FROM SYS_STREAM_SCAN_STATES
WHERE stream_name = 'dev-data-stream'
GROUP BY
    mv_name,
    external_schema_name,
    stream_name
ORDER BY ultima_lectura DESC;


/* HISTORIAL DE REFRESH — consulta ejecutada y validada. */
SELECT *
FROM SYS_MV_REFRESH_HISTORY
WHERE mv_name = 'mv_kinesis_raw'
ORDER BY start_time DESC
LIMIT 5;


/* IDENTIDAD Y PERMISOS — consultas de validación del PDF. */
SELECT CURRENT_USER;

SELECT *
FROM svv_schema_privileges
WHERE namespace_name = 'analytics';

SELECT *
FROM svv_relation_privileges
WHERE namespace_name = 'analytics'
  AND relation_name = 'vw_kinesis_iceberg';

SELECT *
FROM svv_user_grants
WHERE user_name = 'analytics_user';

SELECT *
FROM analytics.vw_kinesis_iceberg
LIMIT 10;
