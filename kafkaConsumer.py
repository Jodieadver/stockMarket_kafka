import json
from confluent_kafka import Consumer
from kafka import KafkaConsumer

BOOTSTRAP = "localhost:9092"  # 或 "localhost:9092"
TOPIC = "GSPC"


consumer = KafkaConsumer(
    "GSPC",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
)



for message in consumer:
    print(f"Received: {message.value}")

   