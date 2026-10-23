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

- **Amazon Kinesis Data Streams:** flujo de datos con dos shards y una retención configurada de 24 horas.
- **Amazon Data Firehose:** entrega los registros del stream a S3.
- **Amazon S3:** almacenamiento de los datos entregados, organizados por año, mes y día.
- **Amazon CloudWatch:** grupo de logs, stream de logs y alarmas para detectar excesos de throughput de lectura y escritura en Kinesis.
- **IAM:** rol de entrega de Firehose con permisos específicos para acceder al stream, escribir en S3 y publicar logs.

La creación efectiva de los recursos de ingesta depende de que la cuenta de AWS tenga habilitado el servicio correspondiente y de que las credenciales dispongan de los permisos necesarios.

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
- `PLAN_OUTPUT.md`: evidencia del plan de Terraform, si corresponde a la versión evaluada.

## Requisitos

- Terraform 1.10.0 o superior.
- AWS CLI configurado con credenciales válidas.
- Cuenta de AWS con acceso a los servicios utilizados.
- Permisos de AWS para consultar y administrar los recursos definidos.

## Comandos de ejecución

Ejecutar desde la raíz del repositorio.

Inicializar Terraform:

```powershell
terraform -chdir=environments/dev init
```

Formatear los archivos:

```powershell
terraform fmt -recursive
```

Validar la configuración:

```powershell
terraform -chdir=environments/dev validate
```

Revisar los cambios propuestos:

```powershell
terraform -chdir=environments/dev plan
```

Aplicar los cambios únicamente después de revisar el plan y confirmar que AWS está habilitado para crear los recursos:

```powershell
terraform -chdir=environments/dev apply
```

Consultar los outputs:

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

La configuración de Terraform fue validada mediante `terraform validate`.

En la última revisión del plan se identificaron siete recursos por crear y una política IAM por actualizar, sin recursos por destruir. Por lo tanto, ese plan todavía contempla cambios pendientes y no representa una infraestructura completamente sincronizada con el código actual.
