import logging
import time
from datetime import date, timedelta

import pandas as pd
import yfinance as yf

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from config.tickers import BANK_TICKERS, DATE_END, DATE_START
from db.connection import get_conn
from db.landing import (
    create_tables,
    get_last_fundamentals_date,
    get_last_holders_date,
    get_last_price_date,
    get_last_recommendations_date,
    get_prev_rows_inserted,
    insert_fundamentals,
    insert_holders,
    insert_ingestion_metrics,
    insert_metadata,
    insert_prices,
    insert_recommendations,
    upsert_company,
)
from utils.anomaly import detect_volume_anomalies
from utils.dataframe import validate_df
from utils.events import (
    DATA_LANDED,
    EXTRACT_ERROR,
    EXTRACTION_FAILED,
    NO_NEW_DATA,
    PRE_CHECK_FAILED,
    VOLUME_ANOMALY,
    emit_event,
)
from utils.metrics import compute_ingestion_metrics
from utils.requests import safe_request


def _on_dag_failure(context):
    task_id = context.get("task_instance", {}).task_id if context.get("task_instance") else "unknown"
    emit_event(EXTRACTION_FAILED, {
        "dag_id": context.get("dag_run", {}).dag_id if context.get("dag_run") else "unknown",
        "task_id": task_id,
        "exception": str(context.get("exception", "")),
    })


def _on_precheck_failure(context):
    ti = context.get("task_instance")
    emit_event(PRE_CHECK_FAILED, {
        "task_id": ti.task_id if ti else "unknown",
        "exception": str(context.get("exception", "")),
    })


def _on_extract_failure(context):
    ti = context.get("task_instance")
    emit_event(EXTRACT_ERROR, {
        "task_id": ti.task_id if ti else "unknown",
        "exception": str(context.get("exception", "")),
    })


@dag(
    dag_id="yfinance_extract_banks_dag",
    dag_display_name="yfinance Extract Banks DAG",
    description="Extracts stock prices, fundamentals, holders and recommendations for US banks from Yahoo Finance and loads them into the PostgreSQL landing zone.",
    schedule=None,
    tags=["extract", "yfinance"],
    on_failure_callback=_on_dag_failure,
)
def yfinance_extract_banks_dag():

    logger = logging.getLogger("airflow.extract")

    trigger_yfinance_precheck = TriggerDagRunOperator(
        task_id="trigger_yfinance_precheck",
        trigger_dag_id="yfinance_pre_check_dag",
        wait_for_completion=True,
        poke_interval=10,
        on_failure_callback=_on_precheck_failure,
    )

    trigger_postgres_precheck = TriggerDagRunOperator(
        task_id="trigger_postgres_precheck",
        trigger_dag_id="postgres_landing_pre_check_dag",
        wait_for_completion=True,
        poke_interval=10,
        on_failure_callback=_on_precheck_failure,
    )

    @task(retries=3, retry_delay=timedelta(seconds=10), retry_exponential_backoff=True,
          on_failure_callback=_on_extract_failure)
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

    @task(retries=3, retry_delay=timedelta(seconds=10), retry_exponential_backoff=True,
          on_failure_callback=_on_extract_failure)
    def fetch_price(last_price_date):
        logger.info("Extracting stock prices")
        results = []

        for t in BANK_TICKERS:
            try:
                if last_price_date:
                    start = str(last_price_date)
                    end = str(date.today())
                else:
                    start = DATE_START
                    end = DATE_END

                logger.info(f"{t} fetching price data {start} to {end}")
                df = safe_request(lambda: yf.download(t, start=start, end=end), t)
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

    @task(retries=3, retry_delay=timedelta(seconds=10), retry_exponential_backoff=True,
          on_failure_callback=_on_extract_failure)
    def fetch_fundamentals(last_fundamentals_date):
        start = str(last_fundamentals_date) if last_fundamentals_date else DATE_START
        end = str(date.today()) if last_fundamentals_date else DATE_END
        logger.info(f"Extracting quarterly fundamentals {start} to {end}")
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
                    if col_date <= start or col_date > end:
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

    @task(retries=3, retry_delay=timedelta(seconds=10), retry_exponential_backoff=True,
          on_failure_callback=_on_extract_failure)
    def fetch_holders(last_holders_date):
        start = str(last_holders_date) if last_holders_date else DATE_START
        logger.info(f"Extracting institutional holders since {start}")
        results = []

        for t in BANK_TICKERS:
            try:
                logger.info(f"{t} fetching holders")
                ticker = yf.Ticker(t)
                df = safe_request(lambda: ticker.institutional_holders, t)
                if df is None or df.empty:
                    logger.warning(f"{t} no holders data found")
                    continue

                df["Date Reported"] = pd.to_datetime(df["Date Reported"]).dt.date
                df = df[df["Date Reported"] > pd.Timestamp(start).date()]
                if df.empty:
                    logger.info(f"{t} no new holders since {start}")
                    continue

                df["Date Reported"] = df["Date Reported"].astype(str)
                logger.info(f"{t} new holders: {len(df)} rows")

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

    @task(retries=3, retry_delay=timedelta(seconds=10), retry_exponential_backoff=True,
          on_failure_callback=_on_extract_failure)
    def fetch_recommendations(last_recommendations_date):
        start = str(last_recommendations_date) if last_recommendations_date else DATE_START
        end = str(date.today()) if last_recommendations_date else DATE_END
        logger.info(f"Extracting analyst recommendations {start} to {end}")
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
                df = df[(df["GradeDate"] > start) & (df["GradeDate"] <= end)]

                if df.empty:
                    logger.info(f"{t} no new recommendations since {start}")
                    continue

                df["GradeDate"] = df["GradeDate"].dt.date.astype(str)

                for _, row in df.iterrows():
                    results.append({
                        "ticker": t,
                        "date": row["GradeDate"],
                        "firm": row["Firm"],
                        "to_grade": row["ToGrade"],
                        "from_grade": row["FromGrade"],
                        "action": row["Action"],
                    })

                logger.info(f"{t} new recommendations since {start}: {len(df)}")

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

        last_price_date = get_last_price_date()
        if last_price_date:
            cutoff = str(last_price_date)
            for row in consolidated:
                row["prices"] = [p for p in (row.get("prices") or []) if p["date"] > cutoff]

        total_new_rows = sum(
            len(row.get("prices") or []) for row in consolidated
        )

        if total_new_rows == 0:
            emit_event(NO_NEW_DATA, {"tickers_checked": len(consolidated)})
            logger.info("No new data to load, skipping insert")
            return {"consolidated": consolidated, "skipped": True}

        metrics = compute_ingestion_metrics(consolidated)
        prev_rows = get_prev_rows_inserted()
        anomalies = detect_volume_anomalies(metrics, prev_rows)
        if anomalies:
            emit_event(VOLUME_ANOMALY, {"anomalies": anomalies})

        with get_conn() as conn:
            with conn.cursor() as cur:
                create_tables(cur)
                for row in consolidated:
                    ticker = row["ticker"]
                    upsert_company(cur, row["basic_info"])
                    insert_prices(cur, ticker, row.get("prices") or [])
                    insert_fundamentals(cur, ticker, row.get("fundamentals") or [])
                    insert_holders(cur, ticker, row.get("holders") or [])
                    insert_recommendations(cur, ticker, row.get("recommendations") or [])
            conn.commit()

        emit_event(DATA_LANDED, {"tickers_loaded": len(consolidated), "price_rows": total_new_rows})
        logger.info(f"Loaded {len(consolidated)} tickers ({total_new_rows} new price rows)")
        return {"consolidated": consolidated, "metrics": metrics, "anomalies": anomalies, "skipped": False}

    @task
    def save_metadata(load_result):
        if load_result.get("skipped"):
            logger.info("No new data, skipping metadata save")
            return

        with get_conn() as conn:
            with conn.cursor() as cur:
                insert_metadata(cur, load_result["consolidated"])
            conn.commit()

        logger.info(f"Metadata saved for {len(load_result['consolidated'])} tickers")

    @task
    def save_metrics(load_result):
        if load_result.get("skipped"):
            logger.info("No new data, skipping metrics save")
            return

        with get_conn() as conn:
            with conn.cursor() as cur:
                create_tables(cur)
                insert_ingestion_metrics(cur, load_result["metrics"], load_result["anomalies"])
            conn.commit()

        logger.info("Ingestion metrics saved to landing")

    @task
    def get_last_price_date_task():
        return get_last_price_date()

    @task
    def get_last_fundamentals_date_task():
        return get_last_fundamentals_date()

    @task
    def get_last_holders_date_task():
        return get_last_holders_date()

    @task
    def get_last_recommendations_date_task():
        return get_last_recommendations_date()

    @task
    def prechecks_done(postgres_result, yfinance_result):
        pass

    join = prechecks_done(trigger_postgres_precheck.output, trigger_yfinance_precheck.output)

    last_price = get_last_price_date_task()
    last_fundamentals = get_last_fundamentals_date_task()
    last_holders = get_last_holders_date_task()
    last_recs = get_last_recommendations_date_task()
    basic = fetch_basic_info()

    (
        join
        >> [last_price, last_fundamentals, last_holders, last_recs, basic]
    )

    price = fetch_price(last_price)
    fundamentals = fetch_fundamentals(last_fundamentals)
    holders = fetch_holders(last_holders)
    recs = fetch_recommendations(last_recs)

    consolidated = consolidate_data(basic, price, fundamentals, holders, recs)
    loaded = load_to_landing(consolidated)
    save_metadata(loaded)
    save_metrics(loaded)


yfinance_extract_banks_dag()
