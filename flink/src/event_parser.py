import json
from datetime import datetime, timezone


def parse_event(raw_event: str) -> dict:
    """
    Deserializa un evento JSON y valida sus campos.
    """
    try:
        event = json.loads(raw_event)

        required_fields = [
            "event_id",
            "event_type",
            "user_id",
            "event_time",
            "amount",
        ]

        for field in required_fields:
            if field not in event:
                raise ValueError(f"Falta el campo obligatorio: {field}")

        event_time = datetime.fromisoformat(
            event["event_time"].replace("Z", "+00:00")
        )

        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)

        amount = float(event["amount"])

        if amount < 0:
            raise ValueError("El importe no puede ser negativo")

        return {
            "event_id": str(event["event_id"]),
            "event_type": str(event["event_type"]),
            "user_id": str(event["user_id"]),
            "event_time": event_time.timestamp() * 1000,
            "amount": amount,
        }

    except (json.JSONDecodeError, TypeError, ValueError) as error:
        raise ValueError(f"Error al procesar el evento: {error}") from error