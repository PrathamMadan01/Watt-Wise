import logging
import json
import uuid
import os
import azure.functions as func # type: ignore
from azure.cosmos import CosmosClient


for key, value in os.environ.items():
    if "COSMOS" in key:
        print(key, "=", value[:10] if value else None)

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

            device_id = data.get("device")
            power_kw = data.get("power_kw")
            timestamp = data.get("timestamp")

            is_anomaly = power_kw > 5

            item = {
                "id": str(uuid.uuid4()),
                "device": device_id,
                "power_kw": power_kw,
                "is_anomaly": is_anomaly,
                "timestamp": timestamp
            }

            container.upsert_item(item)

            logging.info("Successfully written to Cosmos DB")

    except Exception as e:
        logging.error(f"Error: {e}")