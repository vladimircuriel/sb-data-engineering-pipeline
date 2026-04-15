import logging

from airflow.sdk import dag, task
from datetime import timedelta

from utils.events import PRE_CHECK_FAILED, emit_event


def _on_failure(context):
    """Airflow failure callback that emits a ``PRE_CHECK_FAILED`` pipeline event.

    Args:
        context: Airflow task context dict provided automatically on failure.
    """
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
    """Verify that Yahoo Finance is reachable by downloading a test price series.

    Task flow:
        check_yfinance_connection
    """
    logger = logging.getLogger("airflow.pre_checks")

    @task(
        retries=3,
        retry_delay=timedelta(seconds=5),
        retry_exponential_backoff=True,
        on_failure_callback=_on_failure,
    )
    def check_yfinance_connection():
        """Download one day of BAC price data to confirm Yahoo Finance is reachable.

        Raises:
            Exception: If no data is returned or the download raises an error.
        """
        import yfinance as yf
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
