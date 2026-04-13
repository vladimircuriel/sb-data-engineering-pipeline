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


def create_tables(cur) -> None:
    cur.execute(_DDL_YFINANCE_RAW)
    cur.execute(_DDL_RUN_METADATA)


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

    cur.execute(
        """
        INSERT INTO yfinance_run_metadata (tickers_count, latest_price_date, rows_inserted)
        VALUES (%s, %s, %s)
        """,
        (len(consolidated), latest_price_date, len(consolidated)),
    )


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
