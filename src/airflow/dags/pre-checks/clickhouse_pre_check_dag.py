import os
import logging
import requests

from datetime import timedelta
from airflow.sdk import dag, task

from utils.events import PRE_CHECK_FAILED, emit_event


def _on_failure(context):
    ti = context.get("task_instance")
    emit_event(PRE_CHECK_FAILED, {
        "dag_id": "clickhouse_pre_check_dag",
        "task_id": ti.task_id if ti else "unknown",
        "exception": str(context.get("exception", "")),
    })


@dag(
    dag_id="clickhouse_pre_check_dag",
    dag_display_name="ClickHouse Pre-Check DAG",
    description="Test connection to ClickHouse via HTTP interface",
    schedule=None,
    tags=["pre-check", "clickhouse"],
)
def clickhouse_pre_check_dag():

    logger = logging.getLogger("airflow.pre_checks")

    @task(
        retries=3,
        retry_delay=timedelta(seconds=5),
        retry_exponential_backoff=True,
        on_failure_callback=_on_failure,
    )
    def check_clickhouse_connection():
        host = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
        port = os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")
        user = os.environ.get("CLICKHOUSE_USER", "admin")
        password = os.environ.get("CLICKHOUSE_PASSWORD", "admin")
        url = f"http://{host}:{port}/"
        logger.info(f"Testing ClickHouse connection at {url}")

        response = requests.get(url, params={"query": "SELECT 1"}, auth=(user, password))

        if response.status_code != 200:
            raise Exception(f"ClickHouse not reachable. Status: {response.status_code}")

        result = response.text.strip()
        if result != "1":
            raise Exception(f"ClickHouse returned unexpected result: {result}")

        logger.info("ClickHouse connection successful")

    check_clickhouse_connection()


clickhouse_pre_check_dag()
