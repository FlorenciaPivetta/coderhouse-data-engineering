import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def build_record():
    """Genera un evento de ejemplo y una clave de partición."""
    user_id = f"user_{uuid.uuid4().hex}"

    record = {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(["click", "purchase", "view", "signup"]),
        "user_id": user_id,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "amount": round(random.uniform(1, 500), 2),
    }

    # Clave de alta cardinalidad para reducir el riesgo de hot shards.
    partition_key = user_id
    return record, partition_key


def main():
    parser = argparse.ArgumentParser(
        description="Envía eventos de prueba a Amazon Kinesis."
    )
    parser.add_argument(
        "--stream",
        default=os.getenv("KINESIS_STREAM_NAME", "dev-data-stream"),
        help="Nombre del stream de Kinesis.",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_REGION", "us-east-1"),
        help="Región de AWS.",
    )
    parser.add_argument(
        "--profile",
        default=os.getenv("AWS_PROFILE", "terraform-dev"),
        help="Perfil de AWS CLI.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Cantidad de eventos.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
        help="Segundos entre envíos.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Genera eventos localmente sin llamar a AWS.",
    )
    args = parser.parse_args()

    if args.count < 1:
        parser.error("--count debe ser al menos 1.")
    if args.interval < 0:
        parser.error("--interval no puede ser negativo.")

    client = None
    if not args.dry_run:
        session = boto3.Session(
            profile_name=args.profile,
            region_name=args.region,
        )
        client = session.client("kinesis")

    used_keys = set()
    sent = 0

    mode = "Modo local" if args.dry_run else "Envío a AWS"
    print(
        f"{mode}: {args.count} eventos | "
        f"stream={args.stream} | región={args.region}"
    )

    for index in range(args.count):
        record, partition_key = build_record()
        used_keys.add(partition_key)

        if args.dry_run:
            print(
                f"[DRY-RUN] Evento {index + 1}/{args.count} "
                f"| partición={partition_key} "
                f"| JSON={json.dumps(record, ensure_ascii=False)}"
            )
        else:
            try:
                client.put_record(
                    StreamName=args.stream,
                    Data=json.dumps(record).encode("utf-8"),
                    PartitionKey=partition_key,
                )
                sent += 1
                print(f"[OK] Evento {sent}/{args.count} enviado")
            except (BotoCoreError, ClientError) as exc:
                print(f"[ERROR] No se pudo enviar el evento: {exc}")
                raise SystemExit(1)

            if index < args.count - 1 and args.interval:
                time.sleep(args.interval)

    if args.dry_run:
        print(
            f"Prueba local finalizada: {args.count} eventos generados; "
            f"{len(used_keys)} claves de partición únicas."
        )
    else:
        print(f"Finalizado: {sent} eventos enviados correctamente.")


if __name__ == "__main__":
    main()
