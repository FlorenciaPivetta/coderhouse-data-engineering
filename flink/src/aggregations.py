from typing import Dict, Any


class EventAggregator:
    """
    Agrega métricas de eventos por tipo.
    """

    def __init__(self):
        self.state: Dict[str, Dict[str, Any]] = {}

    def process_event(self, event: dict) -> dict:
        event_type = event["event_type"]
        amount = float(event["amount"])

        if event_type not in self.state:
            self.state[event_type] = {
                "event_count": 0,
                "total_amount": 0.0,
            }

        self.state[event_type]["event_count"] += 1
        self.state[event_type]["total_amount"] += amount

        count = self.state[event_type]["event_count"]
        total = self.state[event_type]["total_amount"]

        return {
            "event_type": event_type,
            "event_count": count,
            "total_amount": round(total, 2),
            "average_amount": round(total / count, 2),
        }