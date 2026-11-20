# Capstone – Sistema de procesamiento de eventos en tiempo real

## 1. Objetivo

Implementar una arquitectura de ingeniería de datos en AWS para ingerir,
procesar y almacenar eventos en tiempo real mediante Amazon Kinesis Data
Streams, Amazon Managed Service for Apache Flink, Apache Iceberg, AWS Glue
Data Catalog, Amazon S3 y Amazon Redshift.

La infraestructura se administra mediante Terraform y el procesamiento
se desarrolla utilizando Python y PyFlink.

## 2. Arquitectura

### 2.1 Flujo principal

```text
Productor Python
       |
       v
Kinesis Data Streams
  dev-data-stream
       |
       v
Amazon Managed Service for Apache Flink
  preentrega4-flink
       |
       +----------------------+
       |                      |
       v                      v
Apache Iceberg          Kinesis Output
Glue Data Catalog       dev-flink-stateful-output-v2
       |                      |
       v                      v
Amazon S3               Amazon Redshift
```

El sistema contempla además una ruta independiente de lectura directa
del stream de entrada hacia una materialized view de Redshift.

### 2.2 Componentes

| Componente | Recurso / función |
|---|---|
| Productor | Python con boto3 |
| Ingesta | Kinesis Data Streams: dev-data-stream |
| Procesamiento | Managed Service for Apache Flink |
| Aplicación Flink | preentrega4-flink |
| Runtime | FLINK-1_19 |
| Salida Kinesis | dev-flink-stateful-output-v2 |
| Catálogo | AWS Glue |
| Base de datos Glue | lakehouse_db |
| Formato analítico | Apache Iceberg |
| Almacenamiento | Amazon S3 |
| Consulta analítica | Amazon Redshift Serverless |

### 2.3 Regiones y recursos

- Kinesis, Flink y Glue: us-east-2.
- Almacenamiento Lakehouse S3: us-east-1.
- Bucket Lakehouse: coderhouse-data-flor-dev.
- Prefijo Lakehouse: lakehouse/.
- Redshift workgroup: redshift-dev-wg.
- Redshift database: analytics.

La configuración regional debe respetarse en las conexiones entre
servicios y en las propiedades de los catálogos.

## 3. Ingesta de eventos

El productor se encuentra en:

`producers/kinesis_producer.py`

Características:

- Genera 100 eventos sintéticos.
- Utiliza los tipos purchase, refund, view y click.
- Genera event_id mediante UUID.
- Genera event_time en formato ISO 8601 UTC.
- Utiliza user_id como clave de partición.
- Envía un evento por segundo.
- Utiliza boto3 y la región us-east-2.
- Stream de destino: dev-data-stream.

El productor utiliza las credenciales disponibles mediante la configuración
de AWS SDK. No recibe parámetros de consola: los valores están definidos
en el código.

## 4. Contrato y validación de eventos

El parser se encuentra en:

`flink/src/event_parser.py`

Campos obligatorios:

- event_id
- event_type
- user_id
- event_time
- amount

El parser convierte event_time a milisegundos Unix y amount a valor
numérico. Los importes negativos y los eventos inválidos son rechazados.

La función safe_parse_event permite descartar eventos inválidos sin
interrumpir el procesamiento.

El contrato del productor y el parser fue contrastado mediante inspección
del código.

## 5. Procesamiento con Apache Flink

La aplicación utiliza PyFlink y procesamiento basado en estado.

Se implementan:

- Event Time.
- Watermarks.
- Ventanas Tumbling.
- Ventanas Sliding.
- Agregaciones por tipo de evento.
- Conteo de eventos.
- Importe total.
- Importe promedio.

### Parámetros operativos observados

| Parámetro | Valor |
|---|---|
| Runtime | FLINK-1_19 |
| Parallelism | 1 |
| Checkpoint interval | 60 segundos |
| Min pause between checkpoints | 30 segundos |
| Watermark out-of-orderness | 30 segundos |

Estos valores corresponden a la configuración observada durante las
verificaciones del entorno. Deben contrastarse con la configuración
efectiva de la versión que se utilice como evidencia final.

## 6. Persistencia Lakehouse

El almacenamiento analítico utiliza Apache Iceberg sobre Amazon S3,
con Glue Data Catalog.

Base de datos:

`lakehouse_db`

Tablas implementadas:

- event_window_aggregations
- event_sliding_window_aggregations

La persistencia contempla agregaciones por ventanas y commits de Iceberg.

Se observaron commits y registros almacenados en las tablas. La
reconciliación integral entre todos los eventos de entrada y los resultados
de ambas ventanas todavía requiere evidencia consolidada.

### Idempotencia y prueba manual de UPSERT

Se ejecutó el script:

`flink/tests/test_iceberg_upsert.py`

La prueba se realizó sobre una tabla Iceberg aislada:

`lakehouse_db.event_window_aggregations_test_upsert`

La tabla utiliza la clave lógica:

`(event_type, window_start, window_end)`

Se verificaron escrituras repetidas sobre una misma clave y una
actualización posterior. La lectura lógica final mostró una sola fila
para esa clave, con los valores actualizados.

También se inspeccionaron los snapshots 6 a 10. Cada uno registró un
archivo de datos y un archivo de equality deletes, comportamiento
consistente con las operaciones UPSERT ejecutadas.

**Alcance y limitaciones de la prueba:**

- Las dos ventanas distintas inspeccionadas ya existían en snapshots
  anteriores. Por lo tanto, esta ejecución no demuestra su inserción
  desde una tabla vacía o una línea base limpia.
- La prueba se realizó sobre una tabla aislada y no modificó las tablas
  productivas.
- La inspección de metadatos de las tablas productivas mostró operaciones
  `append` y cero archivos de equality deletes en los snapshots recientes.
- No se verificó que el UPSERT esté habilitado o integrado en el pipeline
  productivo.

La prueba manual de repetición y actualización queda verificada en el
entorno aislado. La idempotencia integral y la integración del UPSERT en
producción siguen pendientes.

También existe una variante local candidata:

`flink/tests/main_iceberg_upsert_candidate.py`

Su existencia no implica que esté desplegada en AWS.

## 7. Integración con Redshift

Se identificaron dos rutas independientes:

### Ruta 1: eventos originales

`dev-data-stream` → Amazon Redshift →
`public.mv_kinesis_events`

Esta vista permite consultar eventos del stream de entrada.

### Ruta 2: resultados procesados por Flink

`dev-flink-stateful-output-v2` → Amazon Redshift →
`public.mv_flink_stateful_output_v2`

Esta vista consume los registros producidos por Flink.

La segunda ruta es la destinada a consultar los resultados stateful
procesados.

En una verificación puntual del 18/11/2026 se observó que la
materialized view `public.mv_flink_stateful_output_v2` pasó de 302 a
402 registros. Los conteos observados por categoría fueron:

- `click`: 28
- `purchase`: 23
- `refund`: 20
- `view`: 29

La última llegada observada fue a las 03:10:32.758 UTC y la actualización
de la vista se registró a las 03:10:33 UTC.

Esta evidencia confirma la lectura y actualización en esa verificación
puntual, pero no demuestra por sí sola la frescura continua ni la
consistencia temporal integral del pipeline.

## 8. Infraestructura como código

La infraestructura se administra mediante Terraform.

Módulos identificados:

- modules/network/
- modules/identity/
- modules/kinesis/
- modules/flink/
- modules/redshift/

Configuración del entorno:

`environments/dev/`

El backend remoto utiliza S3 y bloqueo de estado.

La configuración debe validarse mediante Terraform antes de cualquier
aplicación.

### Comandos de validación

Desde la raíz del repositorio:

```powershell
terraform -chdir=environments/dev fmt -check
terraform -chdir=environments/dev validate
terraform -chdir=environments/dev plan
```

Revisar cuidadosamente el plan antes de aplicar cambios.

La ejecución de terraform plan no demuestra por sí sola que la
infraestructura pueda desplegarse desde cero.

No ejecutar terraform apply ni terraform destroy sin revisar previamente
el impacto sobre los recursos existentes.

## 9. Pruebas automatizadas

Las pruebas se ejecutaron utilizando Python 3.11.9, pytest 9.2.0,
en el entorno Linux de WSL2.

Comando ejecutado desde la raíz del repositorio en WSL2:

```bash
./.venv-linux/bin/python -m pytest -v flink/tests
```

### Resultados registrados

| Archivo | Pruebas aprobadas |
|---|---:|
| test_event_parser.py | 3 |
| test_safe_event_parser.py | 4 |
| test_stateful_pipeline.py | 2 |
| test_stateful_processor.py | 1 |
| test_window_aggregations.py | 11 |
| Total | 21 |

Resultado observado: 21 pruebas aprobadas, 0 fallidas y 9 advertencias.
La ejecución tuvo una duración registrada de 60,15 segundos.

Las pruebas cubrieron validación de eventos, agregaciones, combinación de
estado, asignación temporal, Watermarks, configuración de ventanas y
pipeline stateful.

Durante la ejecución del pipeline stateful se registraron advertencias
de deprecación en dependencias externas, sin fallos de pruebas.

## 10. Observabilidad

La observabilidad utiliza CloudWatch y las métricas disponibles de Kinesis
y Managed Flink.

Se dispone de evidencia de:

- Finalización de checkpoints.
- Registros de ejecución de Flink.
- Commits de Iceberg.
- Métricas de Kinesis.
- Consultas de las materialized views de Redshift.

Para el cierre se debe documentar:

- Iterator Age de Kinesis.
- Estado y duración de checkpoints.
- Errores de procesamiento.
- Frescura de los datos en Redshift.
- Evidencia del Dashboard de Flink.

Las métricas deben presentarse con su período de observación y contexto.

## 11. Seguridad

Se realizó una auditoría de los roles de ejecución de Flink y Redshift.

### Rol de Flink

`dev-flink-execution-role`

- Se revisaron las políticas IAM y la política de confianza.
- No se identificaron políticas administradas adjuntas.
- `glue:DeleteTable` y `s3:DeleteObject` quedaron identificados como permisos candidatos a revisión.

### Rol de Redshift

`redshift-serverless-dev-role`

- Se revisó la política IAM y la política de confianza.
- No se identificaron políticas administradas adjuntas.
- Se observó `kinesis:ListStreams` con recurso `*`.

No se modificaron permisos durante la auditoría.

El alcance no incluyó una revisión integral de SCP, políticas basadas en
recursos ni políticas de sesión. También queda pendiente completar la
revisión de reglas de red y evaluar los permisos identificados.
## 12. Estructura del repositorio

```text
coderhouse-data-engineering/
|
+-- environments/
|   +-- dev/
|       +-- main.tf
|       +-- variables.tf
|       +-- outputs.tf
|       +-- versions.tf
|       +-- .terraform.lock.hcl
|       +-- test_kinesis_events.py
|
+-- modules/
|   +-- network/
|   +-- identity/
|   +-- kinesis/
|   +-- flink/
|   +-- redshift/
|
+-- flink/
|   +-- config/
|   |   +-- application_config.py
|   |   +-- flink_config.py
|   |   +-- kinesis_config.py
|   +-- src/
|   |   +-- aggregations.py
|   |   +-- event_parser.py
|   |   +-- kinesis_sink.py
|   |   +-- kinesis_source.py
|   |   +-- main.py
|   |   +-- stateful_processor.py
|   |   +-- watermarks.py
|   |   +-- window_aggregations.py
|   +-- tests/
|       +-- test_event_parser.py
|       +-- test_iceberg_upsert.py
|       +-- test_safe_event_parser.py
|       +-- test_stateful_pipeline.py
|       +-- test_stateful_processor.py
|       +-- test_window_aggregations.py
|       +-- main_iceberg_upsert_candidate.py
|
+-- producers/
|   +-- kinesis_producer.py
|
+-- sql/
|   +-- 01_external_schema_glue.sql
|   +-- 02_redshift_streaming_ingestion.sql
|   +-- 03_kinesis_iceberg_integration.sql
|   +-- 04_analytics_security.sql
|   +-- 05_monitoring_validation.sql
|
+-- assembly/
|   +-- assembly.xml
|
+-- pom.xml
+-- README.md
+-- README_SQL.md
+-- PLAN_OUTPUT.md
+-- redshift-glue.json
+-- redshift-statement.json
```

## 13. Pendientes para la aceptación final

Los siguientes puntos requieren evidencia adicional antes de considerar
completado el Capstone:

1. Reconciliar eventos procesados y agregaciones Tumbling y Sliding
   entre Kinesis, Flink e Iceberg.
2. Ejecutar o incorporar evidencia de una prueba controlada de eventos
   tardíos y comportamiento de Watermarks.
3. Evaluar la integración del UPSERT en el pipeline productivo y
   validar su comportamiento sobre las tablas de producción, sin
   confundir los resultados de la prueba aislada con una aceptación
   de idempotencia integral.
4. Completar la validación de frescura continua y consistencia temporal
   de la materialized view de resultados Flink en Redshift.
5. Completar la revisión complementaria de permisos IAM identificados
   y reglas de red.
6. Demostrar un despliegue Terraform reproducible desde cero, sin
   intervención manual.
7. Incorporar evidencia del Dashboard de Flink y consultas de Redshift
   con arrival_timestamp actualizado.
8. Documentar el escenario de saturación de shards Kinesis y su detección.
9. Consolidar los trade-offs y parámetros críticos de la arquitectura.
10. Generar el PDF final de entrega a partir de la documentación y
    evidencias verificadas.

La documentación del DAAT presenta una discrepancia de versión:
la portada indica 2.8 y el control documental indica 2.9. Debe
confirmarse la versión definitiva utilizada para la entrega. Su
existencia no implica que todos los criterios de aceptación técnica
estén cerrados.
## 14. Entregable

Documento final:

`Florencia_Pivetta_Capstone_RealTime.pdf`

El documento debe incluir el diagrama de arquitectura, enlaces a los
repositorios, evidencia de funcionamiento, parámetros críticos,
análisis de trade-offs y análisis de fallos.

La aceptación final debe basarse en evidencia verificable y no solamente
en la existencia del código o de los recursos desplegados.
