from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = "kafka-main-kafka-bootstrap.kafka.svc.cluster.local:9092"   # adjust to your NodePort or port-forward
TOPIC = "ads-events"

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",   # or "earliest"
        enable_auto_commit=True,
        group_id="reader-1",
        value_deserializer=lambda v: v.decode("utf-8")
    )

    print(f"Listening to topic: {TOPIC}")
    for msg in consumer:
        print(f"[{msg.partition}:{msg.offset}] {msg.value}")

if __name__ == "__main__":
    main()

