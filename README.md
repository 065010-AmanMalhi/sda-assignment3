# SDA Assignment 3 - NSE StreamPulse

## What this package implements

Assignment 2 established the NSE market-data schema and Kafka topic `nse-equity-ticks`.
Assignment 3 adds the consumer, MySQL storage layer, synthetic continuous stream, and
Grafana dashboard.

Architecture:

    ticker seed CSVs
          |
          v
    producer.py
          |
          v
    Kafka: nse-equity-ticks
          |
          v
    consumer.py
          |
          v
    MySQL: sda_course_a3.market_data
          |
          v
    Grafana: NSE StreamPulse

## Key behavior

The producer streams **ticker-by-ticker**. Every cycle it publishes one fresh record
for each configured ticker, then waits 5 seconds. The timestamp is generated at publish
time, so Grafana's `now-30m` dashboard window continues moving with the stream.

The consumer inserts every message into MySQL.

Grafana is configured for a 5-second dashboard refresh. Therefore, when both producer
and consumer are running, new records should become visible in the dashboard roughly
within the next refresh cycle.

## Files

- `producer.py` - synthetic NSE market producer
- `consumer.py` - Kafka to MySQL consumer
- `requirements.txt` - Python dependencies
- `data/tickers/*.csv` - ticker-by-ticker seed data
- `sql/schema.sql` - optional manual schema setup
- `grafana/NSE_StreamPulse_Grafana_Assignment3.json` - dashboard import file

## Important note about synthetic data

The Assignment 2 report defines a 10-symbol NSE sample schema but does not list all
10 ticker names in the report. This package therefore uses a configurable NSE-style
10-ticker universe, with RELIANCE.NS included because it is explicitly shown in the
Assignment 2 execution evidence. Replace the CSV files if you need to mirror the exact
original ticker list.

## Run

### 1. Start Kafka

Kafka must be available at:

    localhost:9092

Override with:

    set KAFKA_BOOTSTRAP=localhost:9092

### 2. Start MySQL

MySQL must be available at:

    localhost:3306

Default credentials in the package:

    user=root
    password=root
    database=sda_course_a3

Override with environment variables if your MySQL credentials differ:

    set MYSQL_HOST=localhost
    set MYSQL_PORT=3306
    set MYSQL_USER=root
    set MYSQL_PASSWORD=YOUR_PASSWORD
    set MYSQL_DATABASE=sda_course_a3

### 3. Install Python packages

    python -m pip install -r requirements.txt

### 4. Start consumer FIRST

    python consumer.py

The consumer automatically creates the isolated `sda_course_a3.market_data` table. The existing `sda_course` database is not modified.

### 5. Start producer in a second terminal

    python producer.py

It publishes one record per ticker every 5 seconds.

You can change the interval:

    python producer.py --interval 5

For a faster demo:

    python producer.py --interval 2

### 6. Configure Grafana

Import:

    grafana/NSE_StreamPulse_Grafana_Assignment3.json

When Grafana asks for the datasource, select your MySQL datasource.

The dashboard expects the table:

    sda_course.market_data

The dashboard refresh is set to 5 seconds.

## Dashboard

The dashboard contains 8 analytical visualizations plus KPI panels and a live
market-tape table:

1. Live Price Trend
2. Streaming Volume
3. Market Breadth
4. Market Momentum
5. Top Symbols by Traded Volume
6. Biggest Price Movers
7. Intraday Range & Volatility
8. Large-Move Monitor
9. Streamed Observations KPI
10. Average Close KPI
11. Peak Absolute Move KPI
12. Live Market Tape

## Screenshots

<img width="1920" height="906" alt="Screenshot (1166)" src="https://github.com/user-attachments/assets/874f33b1-61dc-46ed-ab6f-6ce114554da0" />

<img width="1920" height="913" alt="Screenshot (1167)" src="https://github.com/user-attachments/assets/571175f2-7320-4a8f-8963-c0d8c451337d" />

<img width="1920" height="911" alt="Screenshot (1168)" src="https://github.com/user-attachments/assets/54b89cb6-62a8-4135-9c16-5dbabb17f4ba" />



