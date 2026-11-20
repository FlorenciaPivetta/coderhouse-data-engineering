/*
CHECKPOINT 6 — REDSHIFT STREAMING INGESTION
Región: us-east-2
Stream: dev-data-stream
Esquema de streaming: kds

Trazabilidad:
- mv_kinesis_raw: EJECUTADA Y VALIDADA según el PDF.
- mv_kinesis_events: RECONSTRUIDA; el SQL original no se recuperó.
*/

-- 1. EJECUTADA Y VALIDADA según Checkpoint 6.
CREATE MATERIALIZED VIEW public.mv_kinesis_raw
AUTO REFRESH YES
AS
SELECT
    approximate_arrival_timestamp,
    partition_key,
    shard_id,
    sequence_number,
    JSON_PARSE(FROM_VARBYTE(kinesis_data, 'utf-8')) AS payload
FROM kds."dev-data-stream";


/*
2. RECONSTRUIDA A PARTIR DE COLUMNAS Y RESULTADOS OBSERVADOS.
El PDF indica que el SQL original de mv_kinesis_events no pudo recuperarse.
La definición siguiente reproduce la reconstrucción incluida en el anexo.
No ejecutar sin revisar si el objeto ya existe y si duplicar la MV tiene sentido
operativo y de costos.
*/

-- RECONSTRUIDA — no es transcripción del DDL original.
-- CREATE MATERIALIZED VIEW public.mv_kinesis_events
-- AUTO REFRESH YES
-- AS
-- SELECT
--     approximate_arrival_timestamp,
--     partition_key,
--     shard_id,
--     sequence_number,
--     JSON_PARSE(FROM_VARBYTE(kinesis_data, 'utf-8')) AS payload
-- FROM kds."dev-data-stream";


/* CONSULTA DE PARSEO JSON — EJECUTADA Y VALIDADA según Checkpoint 6. */
SELECT
    payload.event_id::VARCHAR AS event_id,
    payload.event_time::VARCHAR AS event_timestamp,
    payload.event_type::VARCHAR AS event_type,
    payload.amount::DOUBLE PRECISION AS payload_value
FROM public.mv_kinesis_events
LIMIT 10;

-- event_timestamp y payload_value son alias SQL.
-- Los nombres originales del JSON son event_time y amount.
