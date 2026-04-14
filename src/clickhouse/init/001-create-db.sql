CREATE DATABASE IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.yfinance_raw (
    _airbyte_raw_id       String,
    _airbyte_extracted_at DateTime,
    ticker                String                 NOT NULL,
    extracted_at          DateTime,
    basic_info            String,
    prices                String,
    fundamentals          String,
    holders               String,
    recommendations       String
) ENGINE = MergeTree()
ORDER BY (ticker, extracted_at);

CREATE TABLE IF NOT EXISTS staging.yfinance_run_metadata (
    _airbyte_raw_id       String,
    _airbyte_extracted_at DateTime,
    run_at                DateTime,
    tickers_count         Int32,
    latest_price_date     Date,
    rows_inserted         Int32
) ENGINE = MergeTree()
ORDER BY run_at;

CREATE TABLE IF NOT EXISTS staging.pipeline_events (
    _airbyte_raw_id       String,
    _airbyte_extracted_at DateTime,
    event_type            LowCardinality(String) NOT NULL,
    created_at            DateTime,
    context               String
) ENGINE = MergeTree()
ORDER BY (event_type, created_at);

CREATE TABLE IF NOT EXISTS staging.yfinance_ingestion_metrics (
    _airbyte_raw_id       String,
    _airbyte_extracted_at DateTime,
    run_at                DateTime,
    total_tickers         Int32,
    total_price_rows      Int32,
    completeness_pct      Decimal(5, 2),
    anomalies             String
) ENGINE = MergeTree()
ORDER BY run_at;

CREATE DATABASE IF NOT EXISTS dwh;
