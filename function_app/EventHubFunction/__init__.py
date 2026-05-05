import logging
import json
import uuid
import os
import azure.functions as func  # type: ignore
from azure.cosmos import CosmosClient


logging.info(f"URI: {os.environ.get('COSMOS_URI')}")
logging.info(f"KEY length: {len(os.environ.get('COSMOS_KEY', ''))}")

def main(events):

    try:
        # Initialize Cosmos INSIDE function (important)
        client = CosmosClient(
            os.environ["COSMOS_URI"],
            credential=os.environ["COSMOS_KEY"]
        )

        database = client.get_database_client("telemetrydb")
        container = database.get_container_client("telemetry")

        for event in events:
            body = event.get_body().decode('utf-8')
            logging.info(f"Received event: {body}")

            data = json.loads(body)

            # Extract all telemetry fields from the event
            device_id = data.get("device")
            power_kw = data.get("power_kw")
            voltage = data.get("voltage")
            current_a = data.get("current_a")
            temperature_c = data.get("temperature_c")
            occupancy = data.get("occupancy")
            building_id = data.get("building_id")
            timestamp = data.get("timestamp")

            is_anomaly = power_kw > 5 if power_kw is not None else False

            # Store ALL fields so the Streamlit dashboard can display them
            item = {
                "id": str(uuid.uuid4()),
                "device": device_id,
                "power_kw": power_kw,
                "voltage": voltage,
                "current_a": current_a,
                "temperature_c": temperature_c,
                "occupancy": occupancy,
                "building_id": building_id,
                "is_anomaly": is_anomaly,
                "timestamp": timestamp
            }

            container.upsert_item(item)

            logging.info("Successfully written to Cosmos DB")

    except Exception as e:
        logging.error(f"Error: {e}")