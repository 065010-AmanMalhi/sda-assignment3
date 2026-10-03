"""
Assignment 3 - Kafka Consumer -> MySQL

Consumes nse-equity-ticks and stores each JSON market record in MySQL.
The consumer creates the database/table automatically if they do not exist.

Grafana reads this table and refreshes every 5 seconds.
"""

import json
import os
from datetime import datetime

import mysql.connector
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "nse-equity-ticks")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "sda-assignment3-mysql-consumer")

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "sda_course_a3")

def mysql_server_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
    )

def setup_database():
    conn = mysql_server_connection()
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}`")
    cur.close()
    conn.close()

    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(30) NOT NULL,
            timestamp DATETIME(3) NOT NULL,
            date DATE NOT NULL,
            open DECIMAL(14,2) NOT NULL,
            high DECIMAL(14,2) NOT NULL,
            low DECIMAL(14,2) NOT NULL,
            close DECIMAL(14,2) NOT NULL,
            volume BIGINT NOT NULL,
            prev_close DECIMAL(14,2) NOT NULL,
            pct_change DECIMAL(10,4) NOT NULL,
            exchange VARCHAR(10) NOT NULL,
            ingested_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
            INDEX idx_market_timestamp (timestamp),
            INDEX idx_market_symbol_timestamp (symbol, timestamp),
            INDEX idx_market_exchange_timestamp (exchange, timestamp)
        )
    """)
    conn.commit()
    cur.close()
    return conn

INSERT_SQL = """
INSERT INTO market_data
(symbol, timestamp, date, open, high, low, close, volume,
 prev_close, pct_change, exchange)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

def normalize_timestamp(value):
    # Handles ISO timestamps produced by the producer.
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.replace(tzinfo=None)

def main():
    conn = setup_database()
    cur = conn.cursor()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    print("=" * 72)
    print("SDA ASSIGNMENT 3 | KAFKA -> MYSQL CONSUMER")
    print(f"Kafka    : {KAFKA_BOOTSTRAP}")
    print(f"Topic    : {KAFKA_TOPIC}")
    print(f"Database : {MYSQL_DATABASE}")
    print("Table    : market_data")
    print("=" * 72)

    count = 0
    try:
        for message in consumer:
            r = message.value

            values = (
                r["symbol"],
                normalize_timestamp(r["timestamp"]),
                r["date"],
                r["open"],
                r["high"],
                r["low"],
                r["close"],
                r["volume"],
                r["prev_close"],
                r["pct_change"],
                r.get("exchange", "NSE"),
            )

            cur.execute(INSERT_SQL, values)
            conn.commit()
            count += 1

            print(
                f"[{count:05d}] {r['symbol']:<16} "
                f"close={r['close']:>9.2f} "
                f"move={r['pct_change']:>7.3f}% "
                f"partition={message.partition} offset={message.offset}"
            )

    except KeyboardInterrupt:
        print("\n[STOP] Consumer stopped by user.")
    finally:
        cur.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main()
