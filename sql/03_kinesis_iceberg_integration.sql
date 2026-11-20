/*
CHECKPOINT 6 — INTEGRACIÓN KINESIS + ICEBERG

La consulta histórica usa lakehouse.event_window_aggregations,
expuesta mediante el esquema externo lakehouse asociado a Glue lakehouse_db.

La vista intermedia y la vista analytics se reproducen según el anexo SQL.
El cuerpo del PDF describe la vista de consumo como analytics.vw_kinesis_iceberg.
*/

-- VISTA INTERMEDIA — EJECUTADA Y VALIDADA según el anexo del Checkpoint 6.
CREATE OR REPLACE VIEW public.vw_analytics_kinesis_iceberg AS
SELECT
    k.payload.event_id::VARCHAR AS event_id,
    k.payload.event_time::VARCHAR AS event_timestamp,
    k.payload.event_type::VARCHAR AS event_type,
    k.payload.amount::DOUBLE PRECISION AS payload_value,
    h.historical_event_count,
    h.historical_total_amount,
    h.historical_average_amount
FROM public.mv_kinesis_events k
LEFT JOIN (
    SELECT
        event_type,
        SUM(event_count) AS historical_event_count,
        SUM(total_amount) AS historical_total_amount,
        SUM(total_amount) / NULLIF(SUM(event_count), 0)
            AS historical_average_amount
    FROM lakehouse.event_window_aggregations
    GROUP BY event_type
) h
    ON k.payload.event_type::VARCHAR = h.event_type
WITH NO SCHEMA BINDING;

-- CONSULTA DE VALIDACIÓN — EJECUTADA Y VALIDADA según el PDF.
SELECT *
FROM public.vw_analytics_kinesis_iceberg
LIMIT 10;
