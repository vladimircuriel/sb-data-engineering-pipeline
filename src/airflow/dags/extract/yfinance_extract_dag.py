import logging
import time
from datetime import timedelta

import pandas as pd
import yfinance as yf

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from config.tickers import BANK_TICKERS, DATE_END, DATE_START
from db.connection import get_conn
from db.landing import create_tables, get_last_price_date, insert_metadata, insert_raw, truncate_raw
from utils.dataframe import validate_df
from utils.requests import safe_request


@dag(
    dag_id="yfinance_extract_banks_dag",
    dag_display_name="yfinance Extract Banks DAG",
    description="Extracts stock prices, fundamentals, holders and recommendations for US banks from Yahoo Finance and loads them into the PostgreSQL landing zone.",
    schedule=None,
    tags=["extract", "yfinance"],
)
def yfinance_extract_banks_dag():

    logger = logging.getLogger("airflow.extract")

    trigger_yfinance_precheck = TriggerDagRunOperator(
        task_id="trigger_yfinance_precheck",
        trigger_dag_id="yfinance_pre_check_dag",
        wait_for_completion=True,
        poke_interval=10,
    )

    trigger_postgres_precheck = TriggerDagRunOperator(
        task_id="trigger_postgres_precheck",
        trigger_dag_id="postgres_landing_pre_check_dag",
        wait_for_completion=True,
        poke_interval=10,
    )

    @task(retries=3, retry_delay=timedelta(seconds=10))
    def fetch_basic_info():
        logger.info("Extracting bank company profiles")
        results = []

        for t in BANK_TICKERS:
            try:
                logger.info(f"{t} fetching basic info")
                ticker = yf.Ticker(t)
                info = safe_request(lambda: ticker.info or {}, t)
                if not info:
                    logger.warning(f"{t} no info found")
                    continue

                results.append(
                    {
                        "symbol": t,
                        "industry": info.get("industry"),
                        "sector": info.get("sector"),
                        "employees": info.get("fullTimeEmployees"),
                        "city": info.get("city"),
                        "phone": info.get("phone"),
                        "state": info.get("state"),
                        "country": info.get("country"),
                        "website": info.get("website"),
                        "address": info.get("address1"),
                    }
                )
                logger.info(f"{t} basic info fetched successfully")

                time.sleep(1)

            except Exception:
                logger.exception(f"{t} basic info failed")

        logger.info(f"Bank company profiles extracted. {len(results)}/{len(BANK_TICKERS)} tickers collected")
        return results

    @task(retries=3, retry_delay=timedelta(seconds=10))
    def fetch_price(last_price_date):
        logger.info("Extracting stock prices")
        results = []

        for t in BANK_TICKERS:
            try:
                start = str(last_price_date) if last_price_date else DATE_START
                logger.info(f"{t} fetching price data {start} to {DATE_END}")
                df = safe_request(lambda: yf.download(t, start=start, end=DATE_END), t)
                if df is None or df.empty:
                    logger.warning(f"{t} no price data found")
                    continue

                validate_df(df, "price", t)

                df.columns = [c[0] for c in df.columns]
                df = df.reset_index()
                df["Date"] = df["Date"].astype(str)

                for _, row in df.iterrows():
                    results.append(
                        {
                            "ticker": t,
                            "date": row["Date"],
                            "open": float(row["Open"]) if pd.notna(row["Open"]) else None,
                            "high": float(row["High"]) if pd.notna(row["High"]) else None,
                            "low": float(row["Low"]) if pd.notna(row["Low"]) else None,
                            "close": float(row["Close"]) if pd.notna(row["Close"]) else None,
                            "volume": int(row["Volume"]) if pd.notna(row["Volume"]) else None,
                        }
                    )

                logger.info(f"{t} price fetched: {len(df)} row(s)")

                time.sleep(1)

            except Exception:
                logger.exception(f"{t} price failed")

        logger.info(f"Stock prices extracted. {len(results)} total rows, {len(BANK_TICKERS)} tickers")
        return results

    @task(retries=3, retry_delay=timedelta(seconds=10))
    def fetch_fundamentals():
        logger.info(f"Extracting quarterly fundamentals {DATE_START} to {DATE_END}")
        results = []

        for t in BANK_TICKERS:
            try:
                logger.info(f"{t} fetching quarterly balance sheet")
                ticker = yf.Ticker(t)
                df = safe_request(lambda: ticker.quarterly_balance_sheet, t)
                if df is None or df.empty:
                    logger.warning(f"{t} no balance sheet data found")
                    continue

                for col in df.columns:
                    col_date = pd.Timestamp(col).date().isoformat()
                    if col_date < DATE_START or col_date > DATE_END:
                        continue

                    snapshot = df[col]
                    assets = snapshot.get("Total Assets")
                    debt = snapshot.get("Total Debt")
                    shares = snapshot.get("Share Issued")
                    entry = {
                        "ticker": t,
                        "date": col_date,
                        "assets": float(assets) if pd.notna(assets) else None,
                        "debt": float(debt) if pd.notna(debt) else None,
                        "shares": float(shares) if pd.notna(shares) else None,
                    }
                    results.append(entry)
                    logger.info(f"{t} fundamentals {col_date}: {entry}")

                time.sleep(1)

            except Exception:
                logger.exception(f"{t} fundamentals failed")

        logger.info(f"Quarterly fundamentals extracted. {len(results)} rows, {len(BANK_TICKERS)} tickers")
        return results

    @task(retries=3, retry_delay=timedelta(seconds=10))
    def fetch_holders():
        logger.info("Extracting institutional holders")
        results = []

        for t in BANK_TICKERS:
            try:
                logger.info(f"{t} fetching holders")
                ticker = yf.Ticker(t)
                df = safe_request(lambda: ticker.institutional_holders, t)
                if df is None or df.empty:
                    logger.warning(f"{t} no holders data found")
                    continue

                df["Date Reported"] = df["Date Reported"].astype(str)
                logger.info(f"{t} holders found: {len(df)} rows")

                for _, row in df.iterrows():
                    shares = row["Shares"]
                    value = row["Value"]
                    results.append(
                        {
                            "ticker": t,
                            "holder": row["Holder"],
                            "shares": int(shares) if pd.notna(shares) else None,
                            "value": float(value) if pd.notna(value) else None,
                            "date": row["Date Reported"],
                        }
                    )

                time.sleep(1)

            except Exception:
                logger.exception(f"{t} holders failed")

        logger.info(f"Institutional holders extracted. {len(results)} holder records collected")
        return results

    @task(retries=3, retry_delay=timedelta(seconds=10))
    def fetch_recommendations():
        logger.info(f"Extracting analyst recommendations {DATE_START} to {DATE_END}")
        results = []

        for t in BANK_TICKERS:
            try:
                logger.info(f"{t} fetching recommendations")
                ticker = yf.Ticker(t)
                df = safe_request(lambda: ticker.upgrades_downgrades, t)
                if not isinstance(df, pd.DataFrame) or df.empty:
                    logger.warning(f"{t} no recommendations found")
                    continue

                df = df.reset_index()
                df["GradeDate"] = pd.to_datetime(df["GradeDate"])
                df = df[(df["GradeDate"] >= DATE_START) & (df["GradeDate"] <= DATE_END)]

                if df.empty:
                    logger.warning(f"{t} no recommendations in date range")
                    continue

                df["GradeDate"] = df["GradeDate"].astype(str)

                for _, row in df.iterrows():
                    entry = {
                        "ticker": t,
                        "date": row["GradeDate"],
                        "to_grade": row["ToGrade"],
                        "from_grade": row["FromGrade"],
                        "action": row["Action"],
                    }
                    results.append(entry)

                logger.info(f"{t} recommendations in range: {len(df)}")

                time.sleep(1)

            except Exception:
                logger.exception(f"{t} recommendations failed")

        logger.info(f"Analyst recommendations extracted. {len(results)}/{len(BANK_TICKERS)} tickers collected")
        return results

    @task(retries=2, retry_delay=timedelta(seconds=10))
    def consolidate_data(basic_info, prices, fundamentals, holders, recommendations):
        logger.info("Consolidating all extracted data")

        def group_by_ticker(data: list[dict]) -> dict:
            grouped: dict = {}
            for r in data:
                grouped.setdefault(r["ticker"], []).append(r)
            return grouped

        prices_by_ticker = group_by_ticker(prices)
        fund_by_ticker = group_by_ticker(fundamentals)
        holders_by_ticker = group_by_ticker(holders)
        recs_by_ticker = group_by_ticker(recommendations)

        results = []
        for row in basic_info:
            t = row["symbol"]
            results.append({
                "ticker": t,
                "basic_info": row,
                "prices": prices_by_ticker.get(t, []),
                "fundamentals": fund_by_ticker.get(t, []),
                "holders": holders_by_ticker.get(t, []),
                "recommendations": recs_by_ticker.get(t, []),
            })

        logger.info(f"Consolidated {len(results)} tickers")
        return results

    @task(retries=3, retry_delay=timedelta(seconds=10))
    def load_to_landing(consolidated):

        logger.info("Loading consolidated data to PostgreSQL landing zone")

        with get_conn() as conn:
            with conn.cursor() as cur:
                create_tables(cur)
                truncate_raw(cur)
                insert_raw(cur, consolidated)
            conn.commit()

        logger.info(f"Loaded {len(consolidated)} rows into yfinance_raw")

    @task
    def save_metadata(consolidated):
        with get_conn() as conn:
            with conn.cursor() as cur:
                insert_metadata(cur, consolidated)
            conn.commit()

        logger.info(f"Metadata saved for {len(consolidated)} tickers")

    @task
    def get_last_price_date_task():
        return get_last_price_date()

    last_date = get_last_price_date_task()
    basic = fetch_basic_info()
    price = fetch_price(last_date)
    fundamentals = fetch_fundamentals()
    holders = fetch_holders()
    recs = fetch_recommendations()
    consolidated = consolidate_data(basic, price, fundamentals, holders, recs)
    loaded = load_to_landing(consolidated)
    meta = save_metadata(consolidated)

    trigger_postgres_precheck >> trigger_yfinance_precheck >> last_date >> [basic, fundamentals, holders, recs]
    [basic, price, fundamentals, holders, recs] >> consolidated >> loaded >> meta


yfinance_extract_banks_dag()
