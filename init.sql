CREATE DATABASE IF NOT EXISTS stock_db;
USE stock_db;

CREATE TABLE IF NOT EXISTS kline_1min (
    symbol VARCHAR(20),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    open_price DECIMAL(10,2),
    high_price DECIMAL(10,2),
    low_price DECIMAL(10,2),
    close_price DECIMAL(10,2),
    volume BIGINT,
    PRIMARY KEY (symbol, window_start)
);