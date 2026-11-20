import json
import math
from datetime import datetime, timezone


def parse_event(raw_event: str) -> dict:
    """
    Deserializa un evento JSON y valida sus campos.
    """
    try:
        event = json.loads(raw_event)

        if not isinstance(event, dict):
            raise ValueError("El evento JSON debe ser un objeto")

        required_fields = [
            "event_id",
            "event_type",
            "user_id",
            "event_time",
            "amount",
        ]

        for field in required_fields:
            if field not in event:
                raise ValueError(
                    f"Falta el campo obligatorio: {field}"
                )

        raw_event_time = event["event_time"]

        if not isinstance(raw_event_time, str) or not raw_event_time.strip():
            raise ValueError(
                "event_time debe ser una cadena de fecha válida"
            )

        event_time = datetime.fromisoformat(
            raw_event_time.replace("Z", "+00:00")
        )

        if event_time.tzinfo is None:
            event_time = event_time.replace(
                tzinfo=timezone.utc
            )

        amount = float(event["amount"])

        if not math.isfinite(amount):
            raise ValueError(
                "El importe debe ser un número finito"
            )

        if amount < 0:
            raise ValueError("El importe no puede ser negativo")

        return {
            "event_id": str(event["event_id"]),
            "event_type": str(event["event_type"]),
            "user_id": str(event["user_id"]),
            "event_time": int(event_time.timestamp() * 1000),
            "amount": amount,
        }

    except (json.JSONDecodeError, TypeError, ValueError) as error:
        raise ValueError(
            f"Error al procesar el evento: {error}"
        ) from error


def safe_parse_event(raw_event: str):
    """
    Procesa eventos válidos y descarta los inválidos
    sin detener el procesamiento de Flink.
    """
    try:
        return parse_event(raw_event)

    except ValueError as error:
        print(f"[EVENTO DESCARTADO] {error}")
        return None
