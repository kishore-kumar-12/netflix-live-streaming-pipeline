#!/usr/bin/env python3
"""
Live Netflix-style producer for Kafka (updates existing catalog of titles).
Sends JSON messages to topic 'netflix_live' on localhost:9092.
"""

import json
import time
import random
import signal
from datetime import datetime, timezone
from kafka import KafkaProducer

# ---------- CONFIG ----------
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "netflix_live"
NUM_TITLES = 100           # you chose option C
UPDATE_INTERVAL_MIN = 2.0  # seconds
UPDATE_INTERVAL_MAX = 5.0  # seconds
# ----------------------------

running = True

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def create_catalog(n):
    """Create n synthetic Netflix titles with initial stats."""
    genres = ["Drama", "Comedy", "Action", "Romance", "Documentary",
              "Sci-Fi", "Horror", "Thriller", "Animation", "Family"]
    countries = ["US", "UK", "IN", "CA", "AU", "FR", "DE", "KR", "JP", "BR"]

    catalog = []
    for i in range(1, n+1):
        title = {
            "id": f"T{i:04d}",
            "title": f"Title {i:03d}",
            "type": "Movie" if random.random() < 0.6 else "TV Show",
            "genre": random.choice(genres),
            "release_year": random.randint(1990, 2024),
            "country": random.choice(countries),
            # realistic-ish base viewers
            "current_viewers": random.randint(50, 10000),
            "total_views": random.randint(1000, 1000000),
            "trending_score": round(random.random() * 100, 2),
            "last_updated": iso_now()
        }
        catalog.append(title)
    return catalog

def update_record(rec):
    """Mutate a single record to simulate live viewer fluctuations."""
    # viewers change by a percentage or an absolute jitter
    change_type = random.random()
    if change_type < 0.6:
        # small relative change
        pct = random.uniform(-0.05, 0.15)   # -5% to +15%
        delta = int(rec["current_viewers"] * pct)
    else:
        # absolute jitter
        delta = random.randint(-20, 2000)

    # ensure viewers never negative
    new_viewers = max(0, rec["current_viewers"] + delta)

    # trending score: prefer recent increase and viewer velocity
    viewer_velocity = new_viewers - rec["current_viewers"]
    trending = rec["trending_score"] + (viewer_velocity / (rec["current_viewers"]+1)) * 10
    # small random drift
    trending += random.uniform(-2, 5)
    trending = max(0.0, min(trending, 100.0))

    rec["current_viewers"] = new_viewers
    rec["total_views"] += max(0, delta)
    rec["trending_score"] = round(trending, 2)
    rec["last_updated"] = iso_now()

    return rec

def sigint_handler(signum, frame):
    global running
    print("\nShutting down producer...")
    running = False

def main():
    global running
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)

    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP} ...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
        retries=3
    )

    catalog = create_catalog(NUM_TITLES)
    print(f"Catalog created: {len(catalog)} titles. Starting updates...")

    # We'll update randomly chosen records in continuous loop
    try:
        while running:
            # choose between 1 and 5 updates per cycle to vary throughput
            updates_this_cycle = random.randint(1, 5)
            for _ in range(updates_this_cycle):
                idx = random.randrange(len(catalog))
                rec = catalog[idx]
                updated = update_record(rec.copy())  # copy to avoid side effects if needed

                # message payload
                payload = {
                    "id": updated["id"],
                    "title": updated["title"],
                    "type": updated["type"],
                    "genre": updated["genre"],
                    "release_year": updated["release_year"],
                    "country": updated["country"],
                    "current_viewers": updated["current_viewers"],
                    "total_views": updated["total_views"],
                    "trending_score": updated["trending_score"],
                    "last_updated": updated["last_updated"]
                }

                # send to kafka topic
                producer.send(TOPIC, value=payload)
                # ephemeral debug print (comment out if too verbose)
                print(f"[{payload['last_updated']}] Sent {payload['id']} viewers={payload['current_viewers']} trending={payload['trending_score']}")

            # flush occasionally to push to broker
            producer.flush()

            # sleep for a randomized interval to simulate real updates
            sleep_time = random.uniform(UPDATE_INTERVAL_MIN, UPDATE_INTERVAL_MAX)
            time.sleep(sleep_time)

    except Exception as ex:
        print("Producer encountered exception:", ex)
    finally:
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass
        print("Producer closed cleanly.")

if __name__ == "__main__":
    main()
