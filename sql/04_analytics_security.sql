/*
CHECKPOINT 6 — SEGURIDAD Y CAPA DE CONSUMO ANALÍTICO

El PDF describe estas sentencias como ejecutadas/validadas.
Revisar primero la existencia de schemas, roles, vistas y usuarios.
No contiene contraseñas.
*/

-- Esquema y rol analítico.
CREATE SCHEMA IF NOT EXISTS analytics;

-- En el documento original: CREATE ROLE analytics_role;
-- Ejecutar solo si el rol no existe.
CREATE ROLE analytics_role;

GRANT USAGE ON SCHEMA analytics TO ROLE analytics_role;

-- Vista de consumo publicada.
CREATE OR REPLACE VIEW analytics.vw_kinesis_iceberg AS
SELECT *
FROM public.vw_analytics_kinesis_iceberg
WITH NO SCHEMA BINDING;

GRANT SELECT ON TABLE analytics.vw_kinesis_iceberg
TO ROLE analytics_role;

-- Usuario de prueba: el PDF indica que fue creado sin contraseña incluida.
-- Ejecutar solo en entorno de prueba y si el usuario no existe.
CREATE USER analytics_user PASSWORD DISABLE;

GRANT ROLE analytics_role TO analytics_user;

-- Permisos de lectura sobre la fuente externa Iceberg.
GRANT USAGE ON SCHEMA lakehouse TO ROLE analytics_role;

GRANT SELECT ON TABLE lakehouse.event_window_aggregations_upsert
TO ROLE analytics_role;

-- Validación descrita en el PDF:
-- CURRENT_USER = analytics_user.
-- SELECT sobre analytics.vw_kinesis_iceberg devolvió 10 registros.
