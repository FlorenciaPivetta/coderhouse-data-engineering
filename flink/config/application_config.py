
import json
import os
from pathlib import Path


def load_application_properties():
    """
    Lee las propiedades de ejecución de Amazon Managed Flink.
    """

    properties_path = Path(
        os.getenv(
            "FLINK_APPLICATION_PROPERTIES_FILE",
            "/etc/flink/application_properties.json"
        )
    )

    if not properties_path.exists():
        return {}

    with properties_path.open("r", encoding="utf-8") as file:
        groups = json.load(file)

    properties = {}

    for group in groups:
        group_id = group.get("PropertyGroupId")
        property_map = group.get("PropertyMap", {})

        if group_id:
            properties[group_id] = property_map

    return properties


def get_application_property(properties, group, key, default=None):
    """
    Obtiene una propiedad específica de un grupo.
    """

    return properties.get(group, {}).get(key, default)
