# CoderHouse Data Engineering

## Objetivo

Implementar una infraestructura base en AWS utilizando Terraform, con una red privada, acceso a S3 mediante un Gateway Endpoint y roles IAM para servicios de datos y auditoría. El proyecto incorpora un módulo de ingesta de datos basado en Amazon Kinesis Data Streams y Amazon Data Firehose para entregar datos en S3.

## Arquitectura

### Infraestructura base

- **VPC:** red virtual del entorno `dev`.
- **Subredes privadas:** dos subredes distribuidas en zonas de disponibilidad.
- **Tabla de rutas:** asociada a las subredes privadas.
- **S3 Gateway Endpoint:** permite acceder a S3 desde la VPC sin requerir una ruta pública a Internet para ese servicio.
- **Data Service Role:** rol IAM para un servicio de datos, con permisos limitados al prefijo configurado en S3.
- **Audit Read-only Role:** rol IAM con permisos de consulta para EC2, IAM y S3, restringidos según las acciones y los recursos compatibles.

### Ingesta de datos

- **Amazon Kinesis Data Streams:** flujo de datos configurado con dos shards y una retención de 24 horas.
- **Amazon Data Firehose:** entrega los registros de Kinesis a S3.
- **Amazon S3:** almacenamiento de los datos entregados, organizados por año, mes y día.
- **Amazon CloudWatch:** grupo y stream de logs, junto con alarmas para detectar excesos de throughput de lectura y escritura en Kinesis.
- **IAM:** rol de entrega de Firehose con permisos específicos para acceder al stream, escribir en S3 y publicar logs.

La creación efectiva de los recursos de ingesta depende de que la cuenta de AWS tenga habilitado el acceso a los servicios correspondientes y de que las credenciales dispongan de los permisos necesarios.

## Backend remoto de Terraform

El estado de Terraform se almacena en un bucket S3 con cifrado SSE-S3 (AES-256), versionado y bloqueo de acceso público. El backend utiliza el mecanismo nativo de bloqueo de S3 mediante `use_lockfile = true`.

- **Región:** `us-east-1`
- **Bucket:** `coderhouse-data-tfstate-662241429835-us-east-1`
- **Clave del estado:** `coderhouse-data-engineering/dev/terraform.tfstate`
- **Bloqueo:** archivo de bloqueo nativo de S3.

## Estructura del proyecto

- `modules/network/`: recursos de red y conectividad privada.
- `modules/identity/`: roles y políticas IAM.
- `modules/kinesis/`: stream de Kinesis, configuración de Firehose, permisos y monitoreo.
- `environments/dev/`: configuración del entorno de desarrollo.
- `producers/`: productor Python para generar y enviar eventos a Kinesis.
- `PLAN_OUTPUT.md`: archivo de referencia para documentar la salida de un plan de Terraform. Su contenido debe corresponder a la ejecución que se presenta como evidencia.

## Requisitos

- Terraform 1.10.0 o superior.
- AWS CLI configurado con credenciales válidas.
- Python instalado y disponible mediante el comando `py` en Windows, o mediante el ejecutable correspondiente.
- Dependencias de Python instaladas desde `producers/requirements.txt`.
- Cuenta de AWS con acceso a los servicios utilizados.
- Permisos de AWS para consultar y administrar los recursos definidos.

## Comandos de ejecución

Ejecutar desde la raíz del repositorio.

### Inicializar Terraform

```powershell
terraform -chdir=environments/dev init
```

### Formatear los archivos

```powershell
terraform fmt -recursive
```

### Validar la configuración

```powershell
terraform -chdir=environments/dev validate
```

### Revisar los cambios propuestos

```powershell
terraform -chdir=environments/dev plan
```

Revisar el plan antes de aplicar cualquier cambio. La salida del plan describe los cambios propuestos y no demuestra que los recursos se hayan creado.

### Aplicar los cambios

Ejecutar únicamente después de revisar el plan y confirmar que AWS permite crear los recursos:

```powershell
terraform -chdir=environments/dev apply
```

### Consultar los outputs

```powershell
terraform -chdir=environments/dev output
```

## Seguridad

- No versionar credenciales ni claves de acceso de AWS.
- No versionar archivos de estado de Terraform (`terraform.tfstate` y sus backups).
- No versionar archivos `.tfvars` que contengan información sensible.
- No versionar planes binarios como `tfplan`.
- Mantener `.terraform.lock.hcl` versionado para registrar las versiones de los proveedores.
- Mantener el acceso público a los buckets bloqueado.
- Aplicar el principio de mínimo privilegio en las políticas IAM.

## Validación

### Validaciones locales

- La configuración de Terraform fue validada mediante `terraform validate`.
- El productor Python pasó la comprobación de sintaxis mediante `py_compile`.
- El modo `--dry-run` generó 100 eventos JSON de prueba y utilizó 100 claves de partición distintas.
- La prueba local se realizó sin enviar registros a AWS.

### Estado del despliegue

El acceso a Kinesis está pendiente de resolución en la cuenta de AWS. Por este motivo, la creación y validación real de los recursos de Kinesis, Firehose y CloudWatch, así como la entrega efectiva de registros en S3, quedan pendientes.

La salida de `terraform plan` debe corresponder a la última ejecución utilizada como evidencia. Un plan describe cambios propuestos, pero no demuestra que los recursos se hayan desplegado.

No considerar las pruebas locales como evidencia de entrega efectiva en AWS.

## Productor Python

El productor se encuentra en `producers/producer.py` y permite generar eventos JSON de prueba y enviarlos a un stream de Kinesis.

### Instalar dependencias

Desde la raíz del repositorio:

```powershell
py -m pip install -r .\producers\requirements.txt
```

### Ejecutar una prueba local

```powershell
py .\producers\producer.py --dry-run --count 100
```

Esta ejecución genera 100 eventos localmente sin conectarse a AWS.

### Enviar eventos a Kinesis

Cuando el stream esté disponible y las credenciales tengan los permisos necesarios:

```powershell
py .\producers\producer.py --stream dev-data-stream --region us-east-1 --profile default --count 100 --interval 0.5
```

El perfil `default` corresponde al perfil configurado en AWS CLI para este entorno.

## Prueba de ingesta real en AWS

La prueba de extremo a extremo está pendiente de que se resuelva el acceso a Kinesis en la cuenta de AWS. Los siguientes comandos documentan el procedimiento que se ejecutará cuando el servicio esté disponible; no constituyen evidencia de una entrega real.

### Enviar un evento de prueba mediante AWS CLI

Crear un archivo `event.json` en la raíz del repositorio con el siguiente contenido:

```json
{
  "event_id": "test-001",
  "event_type": "view",
  "user_id": "user-test-001",
  "event_time": "2026-10-23T00:00:00Z",
  "amount": 10.5
}
```

Enviar el evento al stream:

```powershell
aws kinesis put-record --stream-name dev-data-stream --partition-key "user-test-001" --data fileb://event.json --region us-east-1 --profile default
```

### Verificar la entrega en S3

Cuando Firehose haya procesado el registro y entregado el archivo, listar los objetos del prefijo configurado:

```powershell
aws s3 ls s3://coderhouse-data-flor-dev/lakehouse/ --recursive --region us-east-1 --profile default
```

El ejemplo utiliza el bucket `coderhouse-data-flor-dev` y el prefijo `lakehouse`, configurados para el entorno de desarrollo.

### Evidencia pendiente

Para completar la validación de extremo a extremo, se deberá conservar evidencia de:

1. La respuesta de AWS al envío del registro a Kinesis.
2. La existencia de los recursos de ingesta desplegados.
3. La salida real del comando `aws s3 ls` donde se vea el objeto entregado por Firehose, o una captura equivalente de la consola de S3.

Hasta que estas pruebas se realicen satisfactoriamente, la ingesta debe considerarse pendiente de validación en AWS.