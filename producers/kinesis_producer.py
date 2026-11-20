
import json
import time
import uuid
import random
from datetime import datetime, timezone

import boto3

REGION = "us-east-2"
STREAM_NAME = "dev-data-stream"
TOTAL_EVENTS = 100
INTERVAL_SECONDS = 1

kinesis = boto3.client("kinesis", region_name=REGION)

EVENT_TYPES = ["purchase", "refund", "view", "click"]


def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "user_id": f"user_{random.randint(1, 20)}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "amount": round(random.uniform(10, 500), 2)
    }


def main():
    print(f"Enviando {TOTAL_EVENTS} eventos a {STREAM_NAME}...")

    for i in range(TOTAL_EVENTS):
        event = generate_event()

        response = kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(event).encode("utf-8"),
            PartitionKey=event["user_id"]
        )

        print(
            f"[{i + 1}/{TOTAL_EVENTS}] "
            f"Evento enviado: {event['event_id']} | "
            f"Shard: {response['ShardId']}"
        )

        time.sleep(INTERVAL_SECONDS)

    print("Proceso finalizado.")


if __name__ == "__main__":
    main()
