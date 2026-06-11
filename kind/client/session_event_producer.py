#!/usr/bin/env python3

import json
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_BOOTSTRAP = (
    "kafka-main-kafka-bootstrap.kafka.svc.cluster.local:9092"
)

TOPIC = "session-events"


def build_event(user_id: str):

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_type": "click",
        "revenue": 0.1,

        # Event time used by Flink
        "event_time": int(time.time() * 1000),

        # Human readable
        "event_ts": datetime.now(
            timezone.utc
        ).isoformat()
    }


def send_event(producer, user_id):

    event = build_event(user_id)

    producer.send(
        TOPIC,
        key=user_id.encode(),
        value=event
    )

    print(
        f"sent: "
        f"user={user_id} "
        f"time={event['event_ts']}"
    )


def main():

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v:
            json.dumps(v).encode("utf-8"),
        acks="all"
    )

    print("Starting session demo...")

    try:

        #
        # SESSION 1
        #
        print("\nSESSION 1")

        send_event(producer, "alice")
        time.sleep(10)

        send_event(producer, "alice")
        time.sleep(10)

        send_event(producer, "alice")

        #
        # GAP > 30s
        #
        print(
            "\nWaiting 40 seconds "
            "(session should close)"
        )

        time.sleep(40)

        #
        # SESSION 2
        #
        print("\nSESSION 2")

        send_event(producer, "alice")
        time.sleep(10)

        send_event(producer, "alice")

        #
        # GAP > 30s
        #
        print(
            "\nWaiting 40 seconds "
            "(session should close)"
        )

        time.sleep(40)

        #
        # SESSION 3
        #
        print("\nSESSION 3")

        send_event(producer, "alice")

        producer.flush()

        print("\nDone.")

    finally:

        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
