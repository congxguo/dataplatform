import json
import time
import uuid
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "kafka-main-kafka-bootstrap.kafka.svc.cluster.local:9092"   # adjust to NodePort or port-forward
TOPIC = "ads-events"
RATE = 10                            # messages per second

def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user-{uuid.uuid4().hex[:6]}",
        "ad_id": f"ad-{uuid.uuid4().hex[:6]}",
        "timestamp": int(time.time() * 1000),
        "action": "view"
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    interval = 1.0 / RATE
    print(f"Producing {RATE} events/s to topic '{TOPIC}'")

    while True:
        event = generate_event()
        producer.send(TOPIC, event)
        print("Sent:", event)
        time.sleep(interval)

if __name__ == "__main__":
    main()
