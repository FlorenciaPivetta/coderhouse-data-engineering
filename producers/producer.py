import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import BotoCoreError, ClientError


PARTITION_KEYS = ["web", "mobile", "api", "store", "campaign"]


def build_record():
    """Genera un evento de ejemplo para la ingesta."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(["click", "purchase", "view", "signup"]),
        "user_id": f"user_{random.randint(1, 1000):04d}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "amount": round(random.uniform(1, 500), 2),
    }


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
        help="Genera y muestra eventos localmente, sin llamar a AWS.",
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

    print(
        f"{'Modo local' if args.dry_run else 'Envío a AWS'}: "
        f"{args.count} eventos | stream={args.stream} | región={args.region}"
    )

    for index in range(args.count):
        record = build_record()

        # En la prueba local, recorremos las claves para comprobar
        # que se utilicen varias. En AWS, elegimos una clave al azar.
        if args.dry_run:
            partition_key = PARTITION_KEYS[index % len(PARTITION_KEYS)]
        else:
            partition_key = random.choice(PARTITION_KEYS)

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
                print(
                    f"[OK] Evento {sent}/{args.count} "
                    f"| partición={partition_key}"
                )
            except (BotoCoreError, ClientError) as exc:
                print(f"[ERROR] No se pudo enviar el evento: {exc}")
                raise SystemExit(1)

            if index < args.count - 1 and args.interval:
                time.sleep(args.interval)

    if args.dry_run:
        print(
            f"Prueba local finalizada: {args.count} eventos generados. "
            f"Claves utilizadas: {', '.join(sorted(used_keys))}"
        )
    else:
        print(f"Finalizado: {sent} eventos enviados correctamente.")


if __name__ == "__main__":
    main()
