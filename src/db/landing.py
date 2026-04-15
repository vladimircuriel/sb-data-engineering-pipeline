from psycopg2.extras import Json

from db.connection import get_conn

_DDL_YFINANCE_COMPANY = """
    CREATE TABLE IF NOT EXISTS yfinance_company (
        ticker       VARCHAR(10)  PRIMARY KEY,
        industry     VARCHAR,
        sector       VARCHAR,
        employees    INTEGER,
        city         VARCHAR,
        phone        VARCHAR,
        state        VARCHAR,
        country      VARCHAR,
        website      VARCHAR,
        address      VARCHAR,
        extracted_at TIMESTAMPTZ  DEFAULT NOW()
    )
"""

_DDL_YFINANCE_PRICES = """
    CREATE TABLE IF NOT EXISTS yfinance_prices (
        ticker  VARCHAR(10)   NOT NULL,
        date    DATE          NOT NULL,
        open    NUMERIC(18,4),
        high    NUMERIC(18,4),
        low     NUMERIC(18,4),
        close   NUMERIC(18,4),
        volume  BIGINT,
        PRIMARY KEY (ticker, date)
    )
"""

_DDL_YFINANCE_FUNDAMENTALS = """
    CREATE TABLE IF NOT EXISTS yfinance_fundamentals (
        ticker  VARCHAR(10)   NOT NULL,
        date    DATE          NOT NULL,
        assets  NUMERIC(18,2),
        debt    NUMERIC(18,2),
        shares  BIGINT,
        PRIMARY KEY (ticker, date)
    )
"""

_DDL_YFINANCE_HOLDERS = """
    CREATE TABLE IF NOT EXISTS yfinance_holders (
        ticker  VARCHAR(10)   NOT NULL,
        holder  VARCHAR       NOT NULL,
        date    DATE          NOT NULL,
        shares  BIGINT,
        value   NUMERIC(18,2),
        PRIMARY KEY (ticker, holder, date)
    )
"""

_DDL_YFINANCE_RECOMMENDATIONS = """
    CREATE TABLE IF NOT EXISTS yfinance_recommendations (
        ticker     VARCHAR(10)  NOT NULL,
        date       DATE         NOT NULL,
        firm       VARCHAR      NOT NULL,
        to_grade   VARCHAR,
        from_grade VARCHAR,
        action     VARCHAR,
        PRIMARY KEY (ticker, date, firm)
    )
"""

_DDL_RUN_METADATA = """
    CREATE TABLE IF NOT EXISTS yfinance_run_metadata (
        id              SERIAL PRIMARY KEY,
        run_at          TIMESTAMPTZ  DEFAULT NOW(),
        tickers_count   INTEGER      NOT NULL,
        rows_inserted   INTEGER      NOT NULL
    )
"""

_DDL_PIPELINE_EVENTS = """
    CREATE TABLE IF NOT EXISTS pipeline_events (
        id         SERIAL PRIMARY KEY,
        event_type VARCHAR(50)  NOT NULL,
        created_at TIMESTAMPTZ  DEFAULT NOW(),
        context    JSONB
    )
"""

_DDL_INGESTION_METRICS = """
    CREATE TABLE IF NOT EXISTS yfinance_ingestion_metrics (
        id               SERIAL PRIMARY KEY,
        run_at           TIMESTAMPTZ  DEFAULT NOW(),
        total_tickers    INTEGER      NOT NULL,
        total_price_rows INTEGER      NOT NULL,
        completeness_pct NUMERIC(5,2) NOT NULL,
        anomalies        JSONB
    )
"""


def create_tables(cur) -> None:
    cur.execute(_DDL_YFINANCE_COMPANY)
    cur.execute(_DDL_YFINANCE_PRICES)
    cur.execute(_DDL_YFINANCE_FUNDAMENTALS)
    cur.execute(_DDL_YFINANCE_HOLDERS)
    cur.execute(_DDL_YFINANCE_RECOMMENDATIONS)
    cur.execute(_DDL_RUN_METADATA)
    cur.execute(_DDL_INGESTION_METRICS)
    cur.execute(_DDL_PIPELINE_EVENTS)


def upsert_company(cur, row: dict) -> None:
    cur.execute(
        """
        INSERT INTO yfinance_company
            (ticker, industry, sector, employees, city, phone, state, country, website, address)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker) DO UPDATE SET
            industry     = EXCLUDED.industry,
            sector       = EXCLUDED.sector,
            employees    = EXCLUDED.employees,
            city         = EXCLUDED.city,
            phone        = EXCLUDED.phone,
            state        = EXCLUDED.state,
            country      = EXCLUDED.country,
            website      = EXCLUDED.website,
            address      = EXCLUDED.address,
            extracted_at = NOW()
        """,
        (
            row.get("symbol"),
            row.get("industry"),
            row.get("sector"),
            row.get("employees"),
            row.get("city"),
            row.get("phone"),
            row.get("state"),
            row.get("country"),
            row.get("website"),
            row.get("address"),
        ),
    )


def insert_prices(cur, ticker: str, prices: list[dict]) -> None:
    for row in prices:
        cur.execute(
            """
            INSERT INTO yfinance_prices (ticker, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, date) DO NOTHING
            """,
            (ticker, row["date"], row.get("open"), row.get("high"),
             row.get("low"), row.get("close"), row.get("volume")),
        )


def insert_fundamentals(cur, ticker: str, fundamentals: list[dict]) -> None:
    for row in fundamentals:
        cur.execute(
            """
            INSERT INTO yfinance_fundamentals (ticker, date, assets, debt, shares)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ticker, date) DO NOTHING
            """,
            (ticker, row["date"], row.get("assets"), row.get("debt"), row.get("shares")),
        )


def insert_holders(cur, ticker: str, holders: list[dict]) -> None:
    for row in holders:
        cur.execute(
            """
            INSERT INTO yfinance_holders (ticker, holder, date, shares, value)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ticker, holder, date) DO NOTHING
            """,
            (ticker, row["holder"], row["date"], row.get("shares"), row.get("value")),
        )


def insert_recommendations(cur, ticker: str, recommendations: list[dict]) -> None:
    for row in recommendations:
        cur.execute(
            """
            INSERT INTO yfinance_recommendations (ticker, date, firm, to_grade, from_grade, action)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, date, firm) DO NOTHING
            """,
            (ticker, row["date"], row["firm"],
             row.get("to_grade"), row.get("from_grade"), row.get("action")),
        )


def insert_metadata(cur, consolidated: list[dict]) -> None:
    total_price_rows = sum(len(row.get("prices") or []) for row in consolidated)

    cur.execute(
        """
        INSERT INTO yfinance_run_metadata (tickers_count, rows_inserted)
        VALUES (%s, %s)
        """,
        (len(consolidated), total_price_rows),
    )


def insert_event(cur, event_type: str, context: dict | None = None) -> None:
    cur.execute(
        """
        INSERT INTO pipeline_events (event_type, context)
        VALUES (%s, %s)
        """,
        (event_type, Json(context) if context else None),
    )


def insert_ingestion_metrics(cur, metrics: dict, anomalies: list[dict]) -> None:
    cur.execute(
        """
        INSERT INTO yfinance_ingestion_metrics
            (total_tickers, total_price_rows, completeness_pct, anomalies)
        VALUES (%s, %s, %s, %s)
        """,
        (
            metrics["total_tickers"],
            metrics["total_price_rows"],
            metrics["completeness_pct"],
            Json(anomalies) if anomalies else None,
        ),
    )


def get_prev_rows_inserted():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'yfinance_run_metadata'
                )
            """)
            exists = cur.fetchone()
            if not exists or not exists[0]:
                return None

            cur.execute("""
                SELECT rows_inserted
                FROM yfinance_run_metadata
                ORDER BY run_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()
    return row[0] if row else None


def _get_last_date(table: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = %s
                )
            """, (table,))
            exists = cur.fetchone()
            if not exists or not exists[0]:
                return None

            cur.execute(f"SELECT MAX(date) FROM {table}")
            row = cur.fetchone()
    return row[0] if row else None


def get_last_price_date():
    return _get_last_date("yfinance_prices")


def get_last_fundamentals_date():
    return _get_last_date("yfinance_fundamentals")


def get_last_holders_date():
    return _get_last_date("yfinance_holders")


def get_last_recommendations_date():
    return _get_last_date("yfinance_recommendations")
