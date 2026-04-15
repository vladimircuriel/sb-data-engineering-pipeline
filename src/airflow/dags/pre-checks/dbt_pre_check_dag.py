import os
import logging
import requests
import docker

from datetime import timedelta
from airflow.sdk import dag, task

from utils.events import PRE_CHECK_FAILED, NO_NEW_DATA, emit_event


def _on_failure(context):
    """Airflow failure callback that emits a ``PRE_CHECK_FAILED`` pipeline event.

    Args:
        context: Airflow task context dict provided automatically on failure.
    """
    ti = context.get("task_instance")
    emit_event(PRE_CHECK_FAILED, {
        "dag_id": "dbt_pre_check_dag",
        "task_id": ti.task_id if ti else "unknown",
        "exception": str(context.get("exception", "")),
    })


@dag(
    dag_id="dbt_pre_check_dag",
    dag_display_name="dbt Pre-Check DAG",
    description="Verifies ClickHouse is reachable and dbt image exists before dbt run",
    schedule=None,
    tags=["pre-check", "dbt", "clickhouse"],
)
def dbt_pre_check_dag():
    """Verify that ClickHouse is reachable, the dbt Docker image exists, and staging has data.

    Task flow:
        check_clickhouse_connection >> check_staging_has_data >> check_dbt_image
    """
    logger = logging.getLogger("airflow.pre_checks")

    @task(
        retries=3,
        retry_delay=timedelta(seconds=5),
        retry_exponential_backoff=True,
        on_failure_callback=_on_failure,
    )
    def check_clickhouse_connection():
        """Execute ``SELECT 1`` against ClickHouse and assert the response equals ``1``.

        Raises:
            Exception: If ClickHouse is unreachable or returns an unexpected result.
        """
        host = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
        port = os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")
        user = os.environ.get("CLICKHOUSE_USER", "admin")
        password = os.environ.get("CLICKHOUSE_PASSWORD", "admin")
        url = f"http://{host}:{port}/"

        response = requests.get(url, params={"query": "SELECT 1"}, auth=(user, password))

        if response.status_code != 200:
            raise Exception(f"ClickHouse not reachable. Status: {response.status_code}")

        if response.text.strip() != "1":
            raise Exception(f"ClickHouse returned unexpected result: {response.text.strip()}")

        logger.info("ClickHouse connection successful")

    @task(
        retries=3,
        retry_delay=timedelta(seconds=5),
        retry_exponential_backoff=True,
        on_failure_callback=_on_failure,
    )
    def check_dbt_image():
        """Assert that the dbt Docker image built by this project exists locally.

        Raises:
            Exception: If the image is not found in the local Docker daemon.
        """
        compose_project = os.environ.get("COMPOSE_PROJECT_NAME", "sb_pipeline")
        image_name = f"{compose_project}-dbt"

        client = docker.from_env()
        try:
            client.images.get(image_name)
            logger.info(f"dbt image '{image_name}' found locally")
        except docker.errors.ImageNotFound:
            raise Exception(f"dbt image '{image_name}' not found — run 'docker compose build dbt'")

    @task(
        retries=3,
        retry_delay=timedelta(seconds=5),
        retry_exponential_backoff=True,
        on_failure_callback=_on_failure,
    )
    def check_staging_has_data():
        """Verify that at least one ClickHouse staging table contains rows.

        Queries each expected staging table and raises if all are empty,
        emitting a ``NO_NEW_DATA`` event before raising.

        Raises:
            Exception: If a staging table query fails or all tables are empty.
        """
        host = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
        port = os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")
        user = os.environ.get("CLICKHOUSE_USER", "admin")
        password = os.environ.get("CLICKHOUSE_PASSWORD", "admin")
        url = f"http://{host}:{port}/"

        tables = [
            "yfinance_company",
            "yfinance_prices",
            "yfinance_fundamentals",
            "yfinance_holders",
            "yfinance_recommendations",
        ]

        tables_with_data = []
        for table in tables:
            query = f"SELECT count() FROM staging.{table}"
            response = requests.get(url, params={"query": query}, auth=(user, password))

            if response.status_code != 200:
                raise Exception(f"Failed to query staging.{table}: {response.text}")

            count = int(response.text.strip())
            if count > 0:
                tables_with_data.append(table)
                logger.info(f"staging.{table} has {count} rows")
            else:
                logger.info(f"staging.{table} is empty — will be skipped by dbt")

        if not tables_with_data:
            emit_event(NO_NEW_DATA, {"reason": "all_staging_tables_empty"})
            raise Exception("All staging tables are empty — run the extract and sync first")

        logger.info(f"{len(tables_with_data)}/{len(tables)} staging tables have data")

    check_clickhouse_connection() >> check_staging_has_data() >> check_dbt_image()


dbt_pre_check_dag()
