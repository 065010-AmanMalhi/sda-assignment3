-- SDA Assignment 3 isolated database
-- This intentionally does NOT touch the existing sda_course database.

CREATE DATABASE IF NOT EXISTS sda_course_a3;
USE sda_course_a3;

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
);
