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
    """Execute all DDL statements to create landing tables if they do not exist.

    Creates the following tables: ``yfinance_company``, ``yfinance_prices``,
    ``yfinance_fundamentals``, ``yfinance_holders``,
    ``yfinance_recommendations``, ``yfinance_run_metadata``,
    ``yfinance_ingestion_metrics``, and ``pipeline_events``.

    Args:
        cur: An open psycopg2 cursor used to execute the statements.
    """
    cur.execute(_DDL_YFINANCE_COMPANY)
    cur.execute(_DDL_YFINANCE_PRICES)
    cur.execute(_DDL_YFINANCE_FUNDAMENTALS)
    cur.execute(_DDL_YFINANCE_HOLDERS)
    cur.execute(_DDL_YFINANCE_RECOMMENDATIONS)
    cur.execute(_DDL_RUN_METADATA)
    cur.execute(_DDL_INGESTION_METRICS)
    cur.execute(_DDL_PIPELINE_EVENTS)


def upsert_company(cur, row: dict) -> None:
    """Insert or update a company profile row in ``yfinance_company``.

    Uses ``ON CONFLICT (ticker) DO UPDATE`` so that re-running the extraction
    always reflects the latest profile data.

    Args:
        cur: An open psycopg2 cursor.
        row: Dict containing the company fields to persist.  Expected keys:
            ``symbol``, ``industry``, ``sector``, ``employees``, ``city``,
            ``phone``, ``state``, ``country``, ``website``, ``address``.
    """
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
    """Insert daily price rows for a single ticker into ``yfinance_prices``.

    Duplicate ``(ticker, date)`` pairs are silently ignored via
    ``ON CONFLICT DO NOTHING``.

    Args:
        cur: An open psycopg2 cursor.
        ticker: The bank ticker symbol (e.g. ``"BAC"``).
        prices: List of price dicts.  Each dict must contain ``date`` and may
            contain ``open``, ``high``, ``low``, ``close``, ``volume``.
    """
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
    """Insert quarterly fundamental snapshots into ``yfinance_fundamentals``.

    Duplicate ``(ticker, date)`` pairs are silently ignored via
    ``ON CONFLICT DO NOTHING``.

    Args:
        cur: An open psycopg2 cursor.
        ticker: The bank ticker symbol.
        fundamentals: List of fundamental dicts.  Each dict must contain
            ``date`` and may contain ``assets``, ``debt``, ``shares``.
    """
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
    """Insert institutional holder rows into ``yfinance_holders``.

    Duplicate ``(ticker, holder, date)`` triplets are silently ignored via
    ``ON CONFLICT DO NOTHING``.

    Args:
        cur: An open psycopg2 cursor.
        ticker: The bank ticker symbol.
        holders: List of holder dicts.  Each dict must contain ``holder`` and
            ``date`` and may contain ``shares`` and ``value``.
    """
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
    """Insert analyst recommendation rows into ``yfinance_recommendations``.

    Duplicate ``(ticker, date, firm)`` triplets are silently ignored via
    ``ON CONFLICT DO NOTHING``.

    Args:
        cur: An open psycopg2 cursor.
        ticker: The bank ticker symbol.
        recommendations: List of recommendation dicts.  Each dict must contain
            ``date`` and ``firm`` and may contain ``to_grade``, ``from_grade``,
            and ``action``.
    """
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
    """Record a run summary row in ``yfinance_run_metadata``.

    Args:
        cur: An open psycopg2 cursor.
        consolidated: List of per-ticker dicts from the extraction run.  The
            function counts total tickers and total price rows from this list.
    """
    total_price_rows = sum(len(row.get("prices") or []) for row in consolidated)

    cur.execute(
        """
        INSERT INTO yfinance_run_metadata (tickers_count, rows_inserted)
        VALUES (%s, %s)
        """,
        (len(consolidated), total_price_rows),
    )


def insert_event(cur, event_type: str, context: dict | None = None) -> None:
    """Insert a pipeline event row into ``pipeline_events``.

    Args:
        cur: An open psycopg2 cursor.
        event_type: Short label identifying the event (e.g. ``"DATA_LANDED"``).
        context: Optional dict with additional metadata stored as JSONB.
    """
    cur.execute(
        """
        INSERT INTO pipeline_events (event_type, context)
        VALUES (%s, %s)
        """,
        (event_type, Json(context) if context else None),
    )


def insert_ingestion_metrics(cur, metrics: dict, anomalies: list[dict]) -> None:
    """Persist computed ingestion metrics and anomalies in ``yfinance_ingestion_metrics``.

    Args:
        cur: An open psycopg2 cursor.
        metrics: Dict produced by
            :func:`~utils.metrics.compute_ingestion_metrics`.  Required keys:
            ``total_tickers``, ``total_price_rows``, ``completeness_pct``.
        anomalies: List of anomaly dicts produced by
            :func:`~utils.anomaly.detect_volume_anomalies`.  Stored as JSONB;
            ``None`` is written when the list is empty.
    """
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
    """Return the price-row count from the most recent run in ``yfinance_run_metadata``.

    Opens its own connection so it can be called outside of an active
    transaction.

    Returns:
        int | None: The ``rows_inserted`` value of the latest run, or ``None``
        if the table does not exist or has no rows yet.
    """
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
    """Return the maximum ``date`` value from the given landing table.

    Checks whether the table exists before querying it so the function is safe
    to call on a fresh database with no tables yet.

    Args:
        table: Unqualified table name to query (e.g. ``"yfinance_prices"``).

    Returns:
        datetime.date | None: The maximum date found, or ``None`` if the table
        does not exist or contains no rows.
    """
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
    """Return the most recent date stored in ``yfinance_prices``.

    Returns:
        datetime.date | None: Latest price date, or ``None`` if no data exists.
    """
    return _get_last_date("yfinance_prices")


def get_last_fundamentals_date():
    """Return the most recent date stored in ``yfinance_fundamentals``.

    Returns:
        datetime.date | None: Latest fundamentals date, or ``None`` if no data exists.
    """
    return _get_last_date("yfinance_fundamentals")


def get_last_holders_date():
    """Return the most recent date stored in ``yfinance_holders``.

    Returns:
        datetime.date | None: Latest holders date, or ``None`` if no data exists.
    """
    return _get_last_date("yfinance_holders")


def get_last_recommendations_date():
    """Return the most recent date stored in ``yfinance_recommendations``.

    Returns:
        datetime.date | None: Latest recommendations date, or ``None`` if no data exists.
    """
    return _get_last_date("yfinance_recommendations")
