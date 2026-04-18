import time
import json
import random
import uuid
import datetime
import os
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient # type: ignore
from azure.eventhub import EventData # type: ignore

load_dotenv()

CONNECTION_STR = os.getenv("EVENTHUB_CONNECTION_SEND")
EVENT_HUB_NAME = "telemetryhub"

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    eventhub_name=EVENT_HUB_NAME
)

def generate_event(building="B103"):
    meter = f"M-{random.randint(1, 50)}"

    return {
        "id": str(uuid.uuid4()),  # Cosmos requires id
        "device": meter,          # REQUIRED for partition key
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "building_id": building,
        "meter_id": meter,
        "power_kw": round(random.uniform(0.5, 10.0), 2),
        "voltage": round(230 + random.uniform(-5, 5), 2),
        "current_a": round(random.uniform(0.5, 25.0), 2),
        "temperature_c": round(random.uniform(18, 35), 1),
        "occupancy": random.randint(0, 200)
    }


if __name__ == "__main__":
    print("Starting energy telemetry simulator...")
    with producer:
        while True:
            batch = producer.create_batch()
            for _ in range(10):
                batch.add(EventData(json.dumps(generate_event())))
            producer.send_batch(batch)
            print("Sent batch of events")
            time.sleep(1)
