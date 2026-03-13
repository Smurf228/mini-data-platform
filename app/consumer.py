from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "pg.demo.customers",
    bootstrap_servers="localhost:29092",
    auto_offset_reset="earliest",
    group_id="cdc-consumer-v2"
)

print("Listening for CDC events...")

for message in consumer:
    data = json.loads(message.value.decode("utf-8"))

    payload = data.get("payload")

    if payload and payload.get("after"):
        print("\nNEW EVENT")
        print(payload["after"])
