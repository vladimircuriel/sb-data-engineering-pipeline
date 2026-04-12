import logging
import yfinance as yf

from airflow.sdk import dag, task
from datetime import timedelta


@dag(
    dag_id="yfinance_pre_check_dag",
    dag_display_name="yfinance Pre-Check DAG",
    description="Test connection to Yahoo Finance via yfinance",
    schedule=None,
    tags=["pre-check", "test", "yfinance"],
)
def yfinance_pre_check_dag():

    logger = logging.getLogger("airflow.pre_checks")

    @task(
        retries=3,
        retry_delay=timedelta(seconds=5),
        retry_exponential_backoff=True,
    )
    def check_yfinance_connection():
        logger.info("Testing connection to Yahoo Finance using yfinance")

        try:
            data = yf.download("BAC", period="1d")
            if data is None or data.empty:
                raise Exception("No data returned from Yahoo Finance")

        except Exception as e:
            logger.error(f"yfinance connection failed: {str(e)}")
            raise

    check_yfinance_connection()


yfinance_pre_check_dag()
