from psycopg2.extras import Json

from db.connection import get_conn

_DDL_YFINANCE_RAW = """
    CREATE TABLE IF NOT EXISTS yfinance_raw (
        id              SERIAL PRIMARY KEY,
        ticker          VARCHAR(10)  NOT NULL,
        extracted_at    TIMESTAMPTZ  DEFAULT NOW(),
        basic_info      JSONB,
        prices          JSONB,
        fundamentals    JSONB,
        holders         JSONB,
        recommendations  JSONB
    )
"""

_DDL_RUN_METADATA = """
    CREATE TABLE IF NOT EXISTS yfinance_run_metadata (
        id                  SERIAL PRIMARY KEY,
        run_at              TIMESTAMPTZ  DEFAULT NOW(),
        tickers_count       INTEGER      NOT NULL,
        latest_price_date   DATE,
        rows_inserted       INTEGER      NOT NULL
    )
"""

_DDL_PIPELINE_EVENTS = """
    CREATE TABLE IF NOT EXISTS pipeline_events (
        id              SERIAL PRIMARY KEY,
        event_type      VARCHAR(50)  NOT NULL,
        created_at      TIMESTAMPTZ  DEFAULT NOW(),
        context         JSONB
    )
"""

_DDL_INGESTION_METRICS = """
    CREATE TABLE IF NOT EXISTS yfinance_ingestion_metrics (
        id                  SERIAL PRIMARY KEY,
        run_at              TIMESTAMPTZ  DEFAULT NOW(),
        total_tickers       INTEGER      NOT NULL,
        total_price_rows    INTEGER      NOT NULL,
        completeness_pct    NUMERIC(5,2) NOT NULL,
        anomalies           JSONB
    )
"""


def create_tables(cur) -> None:
    cur.execute(_DDL_YFINANCE_RAW)
    cur.execute(_DDL_RUN_METADATA)
    cur.execute(_DDL_INGESTION_METRICS)
    cur.execute(_DDL_PIPELINE_EVENTS)


def truncate_raw(cur) -> None:
    cur.execute("TRUNCATE TABLE yfinance_raw RESTART IDENTITY")


def insert_raw(cur, consolidated: list[dict]) -> None:
    for row in consolidated:
        cur.execute(
            """
            INSERT INTO yfinance_raw
                (ticker, basic_info, prices, fundamentals, holders, recommendations)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                row["ticker"],
                Json(row.get("basic_info")),
                Json(row.get("prices")),
                Json(row.get("fundamentals")),
                Json(row.get("holders")),
                Json(row.get("recommendations")),
            ),
        )


def insert_metadata(cur, consolidated: list[dict]) -> None:
    price_dates = [
        price_row["date"]
        for row in consolidated
        for price_row in (row.get("prices") or [])
        if price_row.get("date")
    ]
    latest_price_date = max(price_dates) if price_dates else None
    total_price_rows = sum(len(row.get("prices") or []) for row in consolidated)

    cur.execute(
        """
        INSERT INTO yfinance_run_metadata (tickers_count, latest_price_date, rows_inserted)
        VALUES (%s, %s, %s)
        """,
        (len(consolidated), latest_price_date, total_price_rows),
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
            if not cur.fetchone()[0]:
                return None

            cur.execute("""
                SELECT rows_inserted
                FROM yfinance_run_metadata
                ORDER BY run_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()
    return row[0] if row else None


def get_last_price_date():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'yfinance_run_metadata'
                )
            """)
            if not cur.fetchone()[0]:
                return None

            cur.execute("""
                SELECT latest_price_date
                FROM yfinance_run_metadata
                ORDER BY run_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()
    return row[0] if row else None
