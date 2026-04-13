import logging

import pandas as pd
import yfinance as yf

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from config.tickers import BANK_TICKERS
from db.landing import get_last_price_date
from utils.dataframe import validate_df
from utils.requests import safe_request


@dag(
    dag_id="yfinance_polling_dag",
    dag_display_name="yfinance Polling DAG",
    description=(
        "Polls the landing zone metadata to detect new trading days. "
        "Triggers the extract DAG only if the number of new days meets the threshold."
    ),
    schedule="0 16 * * 1-5",
    params={"min_new_days": 1},
    catchup=False,
    tags=["polling", "yfinance"],
)
def yfinance_polling_dag():

    logger = logging.getLogger("airflow.polling")

    @task.short_circuit
    def has_enough_new_data(**context):
        min_new_days: int = context["params"]["min_new_days"]
        logger.info(f"Polling — threshold: {min_new_days} new trading day(s) required")

        last_price_date = get_last_price_date()

        if last_price_date is None:
            logger.info("No previous run found in metadata — triggering extract")
            return True
        logger.info(f"Last price date on record: {last_price_date}")

        df = safe_request(
            lambda: yf.download(BANK_TICKERS, start=str(last_price_date), progress=False),
            "BANK_TICKERS",
        )
        if not isinstance(df, pd.DataFrame):
            logger.warning("Bank tickers price data not found or invalid format")
            return False

        if df.empty:
            logger.info("No new price data found since last run")
            return False

        validate_df(df, "polling", "BANK_TICKERS")

        dates = pd.DatetimeIndex(df.index).normalize()
        new_dates = [d.date() for d in dates if d.date() > last_price_date]
        logger.info(f"New trading days since last run: {len(new_dates)} (need {min_new_days})")

        if new_dates:
            close_prices = df["Close"].loc[df.index[dates.date > last_price_date]]
            tickers_with_data = [t for t in close_prices.columns if close_prices[t].notna().any()]
            logger.info(f"Tickers with new data: {len(tickers_with_data)}/{len(BANK_TICKERS)} — {tickers_with_data}")

        return len(new_dates) >= min_new_days

    trigger_extract = TriggerDagRunOperator(
        task_id="trigger_extract",
        trigger_dag_id="yfinance_extract_banks_dag",
        wait_for_completion=False,
    )

    has_enough_new_data() >> trigger_extract


yfinance_polling_dag()
