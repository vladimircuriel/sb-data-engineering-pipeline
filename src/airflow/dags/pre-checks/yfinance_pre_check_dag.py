import logging
import yfinance as yf

from airflow.sdk import dag, task
from datetime import timedelta

from utils.events import PRE_CHECK_FAILED, emit_event


def _on_failure(context):
    ti = context.get("task_instance")
    emit_event(PRE_CHECK_FAILED, {
        "dag_id": "yfinance_pre_check_dag",
        "task_id": ti.task_id if ti else "unknown",
        "exception": str(context.get("exception", "")),
    })


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
        on_failure_callback=_on_failure,
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
