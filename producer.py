"""
Assignment 3 - Synthetic NSE Market Stream Producer

Reads ticker-by-ticker seed CSVs generated from the Assignment 2 market schema,
then continuously creates a fresh one-minute-style market candle for every
ticker and publishes it to Kafka.

Important:
- Messages are sent ticker-by-ticker each cycle.
- Kafka key = symbol.
- Each cycle is one dashboard update batch.
- Every cycle uses the CURRENT timestamp so Grafana's "now-30m" window sees it.
- Default cycle delay = 5 seconds.
"""

import argparse
import csv
import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", "tickers")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "nse-equity-ticks")
CYCLE_DELAY = float(os.getenv("STREAM_INTERVAL", "5"))
PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "3"))

random.seed()

def ensure_topic():
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP,
            client_id="sda-assignment3-admin",
        )
        existing = admin.list_topics()
        if TOPIC not in existing:
            admin.create_topics([
                NewTopic(
                    name=TOPIC,
                    num_partitions=PARTITIONS,
                    replication_factor=1,
                )
            ])
            print(f"[TOPIC] Created {TOPIC} with {PARTITIONS} partitions")
        else:
            print(f"[TOPIC] Using existing topic {TOPIC}")
        admin.close()
    except Exception as exc:
        print(f"[TOPIC] Could not auto-create/inspect topic: {exc}")
        print("[TOPIC] Continuing; Kafka may be configured with auto topic creation.")

def load_seed_states():
    states = {}
    for filename in sorted(os.listdir(DATA_DIR)):
        if not filename.endswith(".csv"):
            continue
        path = os.path.join(DATA_DIR, filename)
        with open(path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        if not rows:
            continue
        row = rows[-1]
        states[row["symbol"]] = {
            "price": float(row["close"]),
            "volume_base": int(float(row["volume"])),
        }
    if not states:
        raise RuntimeError(f"No ticker CSV files found in {DATA_DIR}")
    return states

def make_candle(symbol, state):
    prev_close = state["price"]

    # Small random-walk movement with occasional larger moves so the
    # circuit-monitoring panel has meaningful events to display.
    if random.random() < 0.025:
        move = random.choice([-1, 1]) * random.uniform(0.025, 0.045)
    else:
        move = random.gauss(0, 0.0018)

    close = max(1.0, prev_close * (1 + move))
    open_price = prev_close * (1 + random.gauss(0, 0.0007))

    high = max(open_price, close) * (1 + abs(random.gauss(0, 0.0012)))
    low = min(open_price, close) * (1 - abs(random.gauss(0, 0.0012)))

    volume = max(
        1000,
        int(state["volume_base"] * random.uniform(0.65, 1.35))
    )

    pct_change = ((close - prev_close) / prev_close) * 100
    now = datetime.now(timezone.utc).astimezone()
    timestamp = now.isoformat(timespec="seconds")

    state["price"] = close

    return {
        "symbol": symbol,
        "timestamp": timestamp,
        "date": now.date().isoformat(),
        "open": round(open_price, 2),
        "high": round(high, 2),
        "low": round(low, 2),
        "close": round(close, 2),
        "volume": volume,
        "prev_close": round(prev_close, 2),
        "pct_change": round(pct_change, 4),
        "exchange": "NSE",
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=CYCLE_DELAY,
                        help="Seconds between ticker batches; default 5")
    args = parser.parse_args()

    ensure_topic()
    states = load_seed_states()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
    )

    print("=" * 72)
    print("SDA ASSIGNMENT 3 | NSE SYNTHETIC STREAM PRODUCER")
    print(f"Kafka       : {BOOTSTRAP}")
    print(f"Topic       : {TOPIC}")
    print(f"Tickers     : {len(states)}")
    print(f"Batch delay : {args.interval:.1f}s")
    print("Order       : ticker-by-ticker")
    print("=" * 72)

    cycle = 0
    try:
        while True:
            cycle += 1
            print(f"\n[CYCLE {cycle}] {datetime.now().strftime('%H:%M:%S')}")

            # Explicit ticker-by-ticker streaming.
            for symbol in sorted(states):
                record = make_candle(symbol, states[symbol])
                metadata = producer.send(
                    TOPIC,
                    key=symbol,
                    value=record,
                ).get(timeout=10)

                print(
                    f"{symbol:<16} close={record['close']:>9.2f} "
                    f"move={record['pct_change']:>7.3f}% "
                    f"vol={record['volume']:>9} "
                    f"partition={metadata.partition} offset={metadata.offset}"
                )

            producer.flush()
            print(f"[CYCLE {cycle}] {len(states)} ticker records published.")
            time.sleep(max(0.5, args.interval))

    except KeyboardInterrupt:
        print("\n[STOP] Producer stopped by user.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
