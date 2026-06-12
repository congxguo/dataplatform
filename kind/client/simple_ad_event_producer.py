#!/usr/bin/env python3
"""
Ads Event Producer — continuously publishes synthetic ad events to Kafka.

Usage:
    # Continuous (Ctrl-C to stop):
    python ads_producer.py

    # For exactly 1 hour:
    python ads_producer.py --duration 3600

    # Faster throughput (100 msg/s):
    python ads_producer.py --rate 100
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

# ── Configuration ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = "kafka-main-kafka-bootstrap.kafka.svc.cluster.local:9092"   # adjust to your kind-cluster NodePort / port-forward
TOPIC            = "ads-events"
DEFAULT_RATE     = 0.1                # messages per second
DEFAULT_DURATION = None              # None = run forever

# ── Lookup data ───────────────────────────────────────────────────────────────

AD_TYPES       = ["banner", "video", "native", "interstitial", "rewarded"]
PLATFORMS      = ["ios", "android", "web", "ctv"]
COUNTRIES      = ["SG", "US", "GB", "JP", "IN", "AU", "DE", "FR", "BR", "CA"]
CAMPAIGNS      = [f"campaign_{i:03d}" for i in range(1, 21)]
ADVERTISERS    = [f"advertiser_{i:02d}" for i in range(1, 11)]
DEVICE_TYPES   = ["mobile", "tablet", "desktop", "tv"]
OS_LIST        = ["iOS 17", "Android 14", "Windows 11", "macOS 14", "tvOS 17"]
EVENT_TYPES    = ["impression", "click", "conversion", "video_start",
                  "video_25pct", "video_50pct", "video_75pct", "video_complete"]

# Weighted event distribution (more impressions than clicks)
EVENT_WEIGHTS  = [40, 15, 3, 8, 8, 8, 8, 10]

# ── Event generator ───────────────────────────────────────────────────────────
def generate_ad_event() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "campaign_id": random.choice(
            ["campaign_001", "campaign_002", "campaign_003", "campaign_004", "campaign_005", "campaign_006", "campaign_007"]
        ),
        "user_id": f"user_{random.randint(1,10)}",
        "event_type": random.choice(
            ["impression", "click", "conversion"]
        ),
        "revenue": round(random.uniform(0.01, 1.0), 3),

        # Event time used by Flink
        "event_time": int(time.time() * 1000)
    }


def generate_ad_event_out_of_order():

    now_ms = int(time.time() * 1000)

    if random.random() < 0.2:
        now_ms -= random.randint(
            10_000,
            60_000
        )

    return {
        "event_id": str(uuid.uuid4()),
        "campaign_id": random.choice(
            ["campaign_001", "campaign_002"]
        ),
        "event_type": "click",
        "revenue": round(
            random.uniform(0.01, 0.2),
            3
        ),
        "event_time": now_ms
    }
    

def generate_ad_event_very_late():

    r = random.random()
    now_ms = int(time.time() * 1000)

    if r < 0.1:
        now_ms -= 120_000
    elif r < 0.3:
        now_ms -= 20_000

    return {
        "event_id": str(uuid.uuid4()),
        "campaign_id": random.choice(
            ["campaign_001", "campaign_002"]
        ),
        "event_type": "click",
        "revenue": round(
            random.uniform(0.01, 0.2),
            3
        ),
        "event_time": now_ms
    }

# ── Producer loop ─────────────────────────────────────────────────────────────

def run(rate: int, duration: float | None):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        compression_type="gzip",
        acks="all",
        retries=3,
    )

    interval   = 1.0 / rate
    start      = time.time()
    sent       = 0
    batch_log  = 500   # print stats every N messages

    print(f"[producer] Starting — topic={TOPIC!r}  rate={rate}/s  "
          f"duration={'∞' if duration is None else f'{duration}s'}")

    try:
        while True:
            t0 = time.time()

            # Stop if duration elapsed
            if duration and (t0 - start) >= duration:
                print(f"[producer] Duration reached — {sent} events sent in "
                      f"{t0 - start:.1f}s")
                break

            event = generate_ad_event()
            # Partition by campaign_id so Flink gets ordered per-campaign streams
            key   = event["campaign_id"]

            producer.send(TOPIC, key=key, value=event)
            sent += 1

            if sent % batch_log == 0:
                elapsed = time.time() - start
                print(f"[producer] sent={sent}  elapsed={elapsed:.1f}s  "
                      f"throughput={sent/elapsed:.1f}/s")

            # Throttle to target rate
            elapsed_send = time.time() - t0
            sleep_for    = interval - elapsed_send
            if sleep_for > 0:
                time.sleep(sleep_for)

    except KeyboardInterrupt:
        print(f"\n[producer] Interrupted — {sent} events sent")
    finally:
        producer.flush()
        producer.close()
        print("[producer] Producer closed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ads event Kafka producer")
    parser.add_argument("--rate",     type=float,   default=DEFAULT_RATE,
                        help="Messages per second (default: 10)")
    parser.add_argument("--duration", type=float, default=DEFAULT_DURATION,
                        help="Run duration in seconds (default: forever)")
    args = parser.parse_args()
    run(rate=args.rate, duration=args.duration)
