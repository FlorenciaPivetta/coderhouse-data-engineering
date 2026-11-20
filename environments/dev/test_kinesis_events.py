import boto3
import json
import uuid
import random
import time
from datetime import datetime, timezone

REGION = "us-east-2"
STREAM = "dev-data-stream"
TOTAL_EVENTS = 10

kinesis = boto3.client("kinesis", region_name=REGION)

for i in range(TOTAL_EVENTS):
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(["purchase", "refund", "view", "click"]),
        "user_id": f"test_user_{i + 1}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "amount": round(random.uniform(10, 500), 2)
    }

    response = kinesis.put_record(
        StreamName=STREAM,
        Data=json.dumps(event).encode("utf-8"),
        PartitionKey=event["user_id"]
    )

    print(
        f"[{i + 1}/{TOTAL_EVENTS}] "
        f"Enviado: {event['event_id']} | "
        f"Shard: {response['ShardId']} | "
        f"Sequence: {response['SequenceNumber']}"
    )

    time.sleep(1)

print("Prueba finalizada: 10 eventos enviados.")
