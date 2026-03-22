from kafka import KafkaConsumer
import json
import os

GROUP_ID = os.getenv("CDC_GROUP_ID", "cdc-consumer-live")
OFFSET_RESET = os.getenv("CDC_OFFSET_RESET", "latest")

consumer = KafkaConsumer(
    "pg.demo.customers",
    bootstrap_servers="localhost:29092",
    auto_offset_reset=OFFSET_RESET,
    group_id=GROUP_ID,
)

print("Listening for CDC events...")

for message in consumer:
    data = json.loads(message.value.decode("utf-8"))

    payload = data.get("payload")

    if payload and payload.get("after"):
        print("\nNEW EVENT")
        print(payload["after"])
