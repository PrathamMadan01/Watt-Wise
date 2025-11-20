# simulator.py
# Simulated IoT energy meter event generator for Azure Event Hubs

import json
import time
import random
import uuid
import datetime
from azure.eventhub import EventHubProducerClient, EventData

# Replace with your Event Hubs connection string and hub name
CONNECTION_STR = "<EVENT_HUB_CONNECTION_STRING>"
EVENT_HUB_NAME = "<EVENT_HUB_NAME>"

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
)

def generate_event(building="B103"):
    return {
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "event_id": str(uuid.uuid4()),
        "building_id": building,
        "meter_id": "M-" + str(random.randint(1, 50)),
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
                event = generate_event()
                batch.add(EventData(json.dumps(event)))
            producer.send_batch(batch)
            print("Sent batch of events")
            time.sleep(1)
