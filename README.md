# CoderHouse Data Engineering - Pre-entrega 4
## Procesamiento de eventos en tiempo real con Apache Flink

## Objetivo

Desarrollar y desplegar una aplicación de procesamiento de datos en tiempo real utilizando **Amazon Managed Service for Apache Flink**, integrada con un stream de **Amazon Kinesis Data Streams** existente.

La aplicación consume eventos JSON, interpreta sus marcas temporales, asigna Watermarks y realiza agregaciones mediante ventanas de Event Time. La infraestructura necesaria para ejecutar el procesamiento se administra mediante Terraform.

## Arquitectura

El flujo implementado es el siguiente:

1. **Amazon Kinesis Data Streams:** recibe los eventos y funciona como fuente de datos de la aplicación.
2. **Managed Service for Apache Flink:** consume los registros del stream y ejecuta el procesamiento.
3. **Deserialización:** los eventos JSON se validan y transforman en estructuras utilizables por Flink.
4. **Event Time y Watermarks:** se utilizan las marcas temporales de los eventos para gestionar datos que llegan fuera de orden.
5. **Ventanas temporales:** los eventos se agrupan por tipo y se calculan métricas.
6. **Amazon CloudWatch:** recibe los logs de la aplicación para facilitar el seguimiento de su ejecución.

## Lógica de procesamiento

### Consumo y deserialización

La aplicación utiliza el conector de Kinesis para consumir eventos desde el stream existente `dev-data-stream`.

Cada evento se recibe en formato JSON y se procesa mediante `event_parser.py`.

Se validan los siguientes campos:

- `event_id`: identificador del evento.
- `event_type`: tipo de evento.
- `user_id`: identificador del usuario.
- `event_time`: fecha y hora del evento.
- `amount`: importe asociado al evento.

Los registros inválidos o con importes negativos generan errores de validación.

### Event Time y Watermarks

El procesamiento utiliza **Event Time**, tomando como referencia el momento en que ocurrió el evento y no el momento en que fue recibido.

Se implementa una estrategia de Watermarks con una tolerancia de **30 segundos de desorden** (*bounded out-of-orderness*).

Esto permite procesar eventos que llegan con cierto retraso debido a las condiciones naturales de la red.

### Agregaciones mediante ventanas

El flujo principal utiliza ventanas **Tumbling de un minuto**, agrupando los eventos por `event_type`.

Para cada ventana se calculan las siguientes métricas:

- `event_count`: cantidad de eventos.
- `total_amount`: suma de los importes.
- `average_amount`: promedio de los importes.

Los resultados se emiten mediante la salida de la aplicación Flink.

También se encuentra implementada una alternativa de ventanas Sliding de cinco minutos, con desplazamiento de un minuto. Esta funcionalidad está disponible en el código, pero no está conectada al flujo principal.

### Procesamiento Stateful

Se implementa `StatefulAggregator`, una función basada en `KeyedProcessFunction` que utiliza `ValueState` para mantener el conteo de eventos y el importe acumulado por clave.

Esta implementación permite conservar información entre eventos de una misma clave durante la ejecución del operador.

La función cuenta con pruebas específicas, aunque actualmente no forma parte del flujo principal definido en `main.py`, que utiliza las agregaciones por ventanas Tumbling.

## Tolerancia a fallos y Checkpointing

La aplicación tiene habilitado el mecanismo de checkpointing de Managed Service for Apache Flink.

Configuración implementada:

- Checkpointing habilitado.
- Intervalo de checkpoint: 60.000 ms.
- Pausa mínima entre checkpoints: 30.000 ms.
- Modo de checkpoint: `EXACTLY_ONCE`.
- Máximo de checkpoints concurrentes: 1.
- Timeout: 120.000 ms.
- Tolerancia a fallos: 3.

La configuración del recurso de Managed Flink se encuentra en Terraform. La persistencia y recuperación de checkpoints se gestionan mediante el servicio administrado; no se declara un bucket o una ruta S3 personalizada para checkpoints en esta implementación.

## Infraestructura con Terraform

Se utiliza Terraform para definir y administrar los recursos necesarios para desplegar la aplicación.

El módulo `modules/flink/` define:

- Rol IAM de ejecución para Managed Flink.
- Permisos de lectura sobre el stream de Kinesis.
- Permisos para acceder al artefacto almacenado en S3.
- Permisos para publicar logs en CloudWatch.
- Bucket S3 para almacenar el artefacto de la aplicación.
- Aplicación de Amazon Managed Service for Apache Flink.
- Configuración de checkpoints y monitoreo.
- Configuración de paralelismo.

El módulo `modules/kinesis/` define recursos IAM asociados a servicios de ingesta. El stream utilizado por Flink es existente y se referencia mediante su ARN; no se crea un nuevo Kinesis Data Stream en este módulo.

### Paralelismo y KPUs

La aplicación se configuró con:

| Parámetro | Configuración |
|---|---|
| Runtime | FLINK-1_19 |
| Parallelism | 1 |
| Parallelism per KPU | 1 |
| Autoscaling | Deshabilitado |

La configuración utiliza un paralelismo de 1 y una unidad de paralelismo por KPU. El escalado automático está deshabilitado.

## Empaquetado de la aplicación

El proyecto utiliza Maven para generar el artefacto requerido por Managed Flink.

El proceso incluye:

- Dependencias de Apache Flink.
- Conector de Kinesis compatible con Flink.
- Código fuente Python.
- Archivos de configuración.
- Empaquetado de la aplicación en formato ZIP.

Para generar el artefacto:

```bash
mvn clean package
```

El artefacto generado es `target/flink-kinesis-preentrega4-1.0.0-managed-flink.zip`. Terraform lo almacena en S3 y lo utiliza para configurar la aplicación administrada.

## Pruebas

Se implementaron pruebas para validar la deserialización de eventos JSON, el rechazo de registros inválidos, la actualización del estado mediante `ValueState` y la ejecución de un pipeline local.

Para ejecutarlas:

`source .venv-linux/bin/activate`

`PYTHONPATH=. pytest flink/tests/ -v`

## Despliegue y validación

La infraestructura se administra desde `environments/dev/`.

Los comandos de validación son `terraform fmt -recursive`, `terraform validate` y `terraform plan`.

La aplicación se desplegó en Amazon Managed Service for Apache Flink con runtime `FLINK-1_19`.

La validación contempla el estado de la aplicación, la lectura de registros desde Kinesis, las métricas de procesamiento, la ejecución de checkpoints y los logs de CloudWatch.

## Estructura del proyecto

- `assembly/`: configuración del empaquetado.
- `environments/dev/`: configuración de Terraform del entorno.
- `flink/config/`: configuración de Flink y Kinesis.
- `flink/src/`: código de procesamiento.
- `flink/tests/`: pruebas de la aplicación.
- `modules/flink/`: infraestructura de Managed Flink.
- `modules/kinesis/`: recursos IAM asociados a Kinesis.
- `modules/identity/`: roles y políticas IAM.
- `modules/network/`: infraestructura de red.
- `pom.xml`: configuración de Maven.

## Tecnologías utilizadas

Python, Apache Flink, PyFlink, Amazon Managed Service for Apache Flink, Amazon Kinesis Data Streams, Amazon S3, Amazon CloudWatch, AWS IAM, Terraform, Maven y Pytest.

## Seguridad

No se deben versionar credenciales de AWS, archivos de variables sensibles, estados locales de Terraform ni archivos generados durante la ejecución.

---

**Proyecto:** CoderHouse - Data Engineering
**Checkpoint:** Pre-entrega 4 - Procesamiento en tiempo real
