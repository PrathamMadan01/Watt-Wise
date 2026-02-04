# simulator.py
# Simulated IoT energy meter event generator for Azure Event Hubs

import json
import time
import random
import uuid
import datetime
from azure.eventhub import EventHubProducerClient, EventData

import os
EVENTHUB_CONNECTION_STRING = os.environ["EVENTHUB_CONNECTION_STRING"]

EVENTHUB_NAME = os.environ["EVENTHUB_NAME"]


producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
)

def generate_event(building="B103"):
    meter_id = "M-" + str(random.randint(1, 50))

    base_load = random.uniform(1.0, 6.0)
    occupancy = random.randint(0, 200)

    power_kw = base_load + (occupancy * random.uniform(0.005, 0.02))

    if random.random() < 0.03:
        power_kw *= random.uniform(2.5, 4.0)

    voltage = 230 + random.uniform(-4, 4)
    current_a = (power_kw * 1000) / voltage

    return {
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "event_id": str(uuid.uuid4()),

        "building_id": building,
        "meter_id": meter_id,

        "power": {
            "active_kw": round(power_kw, 2)
        },

        "electrical": {
            "voltage_v": round(voltage, 2),
            "current_a": round(current_a, 2)
        },

        "environment": {
            "temperature_c": round(random.uniform(18, 35), 1)
        },

        "occupancy": occupancy
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
