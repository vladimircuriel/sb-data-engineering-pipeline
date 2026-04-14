CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_id      Int32  NOT NULL,
    full_date    Date   NOT NULL,
    day          UInt8  NOT NULL,
    day_year     UInt16 NOT NULL,
    day_week     UInt8  NOT NULL,
    week_number  UInt8  NOT NULL,
    month        UInt8  NOT NULL,
    year         UInt16 NOT NULL,
    quarter      UInt8  NOT NULL,
    is_holiday   UInt8  NOT NULL
) ENGINE = ReplacingMergeTree()
ORDER BY date_id;

CREATE TABLE IF NOT EXISTS dwh.dim_bank (
    bank_id        String NOT NULL,
    industry       LowCardinality(String),
    sector         LowCardinality(String),
    employee_count Int64,
    city           String,
    phone          String,
    state          LowCardinality(String),
    country        LowCardinality(String),
    website        String,
    address        String
) ENGINE = ReplacingMergeTree()
ORDER BY bank_id;

CREATE TABLE IF NOT EXISTS dwh.dim_holder (
    holder_id   String                 NOT NULL,
    name        String                 NOT NULL,
    holder_type LowCardinality(String) NOT NULL
) ENGINE = ReplacingMergeTree()
ORDER BY holder_id;

CREATE TABLE IF NOT EXISTS dwh.dim_agency_rating (
    agency_id   String NOT NULL,
    agency_name String NOT NULL
) ENGINE = ReplacingMergeTree()
ORDER BY agency_id;

CREATE TABLE IF NOT EXISTS dwh.fact_price (
    bank_id     String         NOT NULL,
    date_id     Int32          NOT NULL,
    open_price  Decimal(18, 4) NOT NULL,
    high_price  Decimal(18, 4) NOT NULL,
    low_price   Decimal(18, 4) NOT NULL,
    close_price Decimal(18, 4) NOT NULL,
    volume      Int64          NOT NULL
) ENGINE = MergeTree()
ORDER BY (bank_id, date_id)
PARTITION BY toYear(toDate(toString(date_id)));

CREATE TABLE IF NOT EXISTS dwh.fact_fundamentals (
    bank_id          String         NOT NULL,
    date_id          Int32          NOT NULL,
    assets           Decimal(18, 2),
    debt             Decimal(18, 2),
    invested_capital Decimal(18, 2),
    share_issued     Int64
) ENGINE = MergeTree()
ORDER BY (bank_id, date_id)
PARTITION BY toYear(toDate(toString(date_id)));

CREATE TABLE IF NOT EXISTS dwh.fact_ratings (
    bank_id    String                 NOT NULL,
    date_id    Int32                  NOT NULL,
    agency_id  String                 NOT NULL,
    to_grade   LowCardinality(String),
    from_grade LowCardinality(String),
    action     LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (bank_id, date_id, agency_id);

CREATE TABLE IF NOT EXISTS dwh.fact_holders (
    bank_id   String         NOT NULL,
    date_id   Int32          NOT NULL,
    holder_id String         NOT NULL,
    shares    Int64,
    value     Decimal(18, 2)
) ENGINE = MergeTree()
ORDER BY (bank_id, date_id, holder_id);
