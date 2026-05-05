import os
import sys
import json
import time
import signal
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.insert(0, os.path.expanduser("~/movie-recommendation-system"))
from config.config import *


running = True
def handle_signal(sig, frame):
    global running
    print("\n[PRODUCER] Shutting down gracefully...")
    running = False
signal.signal(signal.SIGINT,  handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=5,
    )
    print(f"[PRODUCER] Connected to Kafka broker: {KAFKA_BROKER}")
    return producer


def load_ratings():
    print(f"[PRODUCER] Loading ratings from: {RATINGS_FILE}")
    ratings = []
    with open(RATINGS_FILE, "r") as f:
        for line in f:
            parts = line.strip().split("::")
            if len(parts) == 4:
                try:
                    ratings.append({
                        "user_id":   int(parts[0]),
                        "item_id":   int(parts[1]),
                        "rating":    float(parts[2]),
                        "timestamp": int(parts[3])
                    })
                except ValueError:
                    continue
    ratings.sort(key=lambda x: x["timestamp"])
    print(f"[PRODUCER] Loaded {len(ratings):,} ratings, sorted by timestamp.")
    return ratings


def on_send_error(exc):
    print(f"[PRODUCER][ERROR] Failed to send message: {exc}")


def run():
    producer = create_producer()
    ratings  = load_ratings()

    sent      = 0
    errors    = 0
    start     = time.time()

    print(f"[PRODUCER] Starting stream to topic '{KAFKA_TOPIC}'")
    print(f"[PRODUCER] Partitioning strategy: item_id % {KAFKA_NUM_PARTITIONS}")
    print(f"[PRODUCER] Speed: {1/PRODUCER_SLEEP_INTERVAL:.0f} events/sec\n")

    for record in ratings:
        if not running:
            break

        message = {
            "user_id":   record["user_id"],
            "item_id":   record["item_id"],
            "rating":    record["rating"],
            "timestamp": datetime.utcfromtimestamp(
                             record["timestamp"]
                         ).strftime("%Y-%m-%dT%H:%M:%S")
        }

        partition_key = record["item_id"] % KAFKA_NUM_PARTITIONS

        producer.send(
            KAFKA_TOPIC,
            key=partition_key,
            value=message
        ).add_errback(on_send_error)

        sent += 1

        if sent % 1000 == 0:
            elapsed  = time.time() - start
            rate     = sent / elapsed
            print(f"[PRODUCER] Sent: {sent:,} | Rate: {rate:.1f} msg/s | "
                  f"Errors: {errors} | Last item_id: {record['item_id']}")

        time.sleep(PRODUCER_SLEEP_INTERVAL)

    producer.flush()
    producer.close()

    elapsed = time.time() - start
    print(f"\n[PRODUCER] Done. Sent {sent:,} messages in {elapsed:.1f}s")
    print(f"[PRODUCER] Average rate: {sent/elapsed:.1f} msg/s")


if __name__ == "__main__":
    run()
