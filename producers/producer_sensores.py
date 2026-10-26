import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "urban_sensors")
INTERVAL_SECONDS = float(os.getenv("INTERVAL_SECONDS", "2"))

SENSOR_IDS = ["sensor-001", "sensor-002", "sensor-003", "sensor-004"]


def generate_event(sensor_id):
    """Genera una medición simulada de un sensor urbano."""
    return {
        "sensor_id": sensor_id,
        "temperature": round(random.uniform(10, 38), 2),
        "humidity": round(random.uniform(25, 95), 2),
        "air_quality_index": random.randint(0, 200),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=5
    )

    print(f"Enviando eventos al tópico '{KAFKA_TOPIC}'...")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print("Presioná Ctrl+C para detener el productor.")

    try:
        while True:
            sensor_id = random.choice(SENSOR_IDS)
            event = generate_event(sensor_id)

            try:
                metadata = producer.send(
                    KAFKA_TOPIC,
                    key=sensor_id,
                    value=event
                ).get(timeout=10)

                print(
                    f"Enviado a {metadata.topic}, "
                    f"partición {metadata.partition}: "
                    f"{json.dumps(event)}"
                )

            except KafkaError as error:
                print(f"Error al enviar el evento: {error}")

            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nProductor detenido.")

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()