# CoderHouse Data Engineering - Pre-entrega 1

## Objetivo

Implementar una infraestructura base en AWS utilizando Terraform, con una red privada, acceso privado a S3 mediante un Gateway Endpoint y roles IAM para servicios de datos y auditoría.

## Arquitectura implementada

- **VPC:** red virtual del entorno `dev`.
- **Subredes privadas:** dos subredes distribuidas en zonas de disponibilidad.
- **Tabla de rutas:** asociada a las subredes privadas.
- **S3 Gateway Endpoint:** permite el acceso a S3 desde la VPC sin requerir una ruta pública a Internet para ese servicio.
- **Data Service Role:** rol IAM para un servicio de datos, con permisos de lectura y escritura limitados al prefijo configurado en S3.
- **Audit Read-only Role:** rol IAM con permisos de consulta para EC2, IAM y S3.

## Backend remoto de Terraform

El estado se almacena en un bucket S3 con cifrado SSE-S3 (AES-256), versionado y bloqueo de acceso pÃºblico. Terraform utiliza el bloqueo nativo de S3 mediante `use_lockfile = true`.

- **Región:** `us-east-1`
- **Bucket:** `coderhouse-data-tfstate-662241429835-us-east-1`
- **Clave del estado:** `coderhouse-data-engineering/dev/terraform.tfstate`

## Estructura del proyecto

- `modules/network/`: recursos de red.
- `modules/identity/`: roles y políticas IAM.
- `environments/dev/`: configuración del entorno de desarrollo.
- `PLAN_OUTPUT.md`: salida del plan de Terraform.

## Requisitos

- Terraform 1.6 o superior.
- AWS CLI configurado con credenciales válidas.
- Permisos de AWS para consultar y administrar los recursos definidos.

## Comandos de ejecuciÃ³n

Ejecutar desde `environments/dev/`:

```powershell
terraform init
terraform fmt -recursive
terraform validate
terraform plan
terraform apply
```

Para consultar los outputs:

```powershell
terraform output
```

## Seguridad

No versionar credenciales, archivos de estado de Terraform, archivos `.tfvars` con valores sensibles ni planes binarios. Mantener `.terraform.lock.hcl` versionado.

## Validación

La infraestructura fue aplicada correctamente. La última ejecuciÃ³n de `terraform plan` finalizó con el mensaje:

`No changes. Your infrastructure matches the configuration.`

Esto indica que no se detectaron diferencias entre la configuración y la infraestructura existente.
