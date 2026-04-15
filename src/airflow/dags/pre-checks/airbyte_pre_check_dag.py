import os
import requests
import logging

from datetime import timedelta
from airflow.sdk import dag, task
from airflow.models import Connection

from utils.events import PRE_CHECK_FAILED, emit_event


def _on_failure(context):
    """Airflow failure callback that emits a ``PRE_CHECK_FAILED`` pipeline event.

    Args:
        context: Airflow task context dict provided automatically on failure.
    """
    ti = context.get("task_instance")
    emit_event(PRE_CHECK_FAILED, {
        "dag_id": "airbyte_pre_check_dag",
        "task_id": ti.task_id if ti else "unknown",
        "exception": str(context.get("exception", "")),
    })


@dag(
    dag_id="airbyte_pre_check_dag",
    dag_display_name="Airbyte Pre-Check DAG",
    description="Verifies the airbyte_default Airflow connection exists and Airbyte is reachable",
    schedule=None,
    tags=["pre-check", "airbyte"],
)
def airbyte_pre_check_dag():
    """Verify that the ``airbyte_default`` Airflow connection exists and Airbyte is reachable.

    Task flow:
        check_airflow_connection >> check_airbyte_health
    """
    logger = logging.getLogger("airflow.pre_checks")

    @task(
        retries=3,
        retry_delay=timedelta(seconds=5),
        retry_exponential_backoff=True,
        on_failure_callback=_on_failure,
    )
    def check_airflow_connection():
        """Verify that the ``airbyte_default`` connection is registered in Airflow.

        Raises:
            AirflowNotFoundException: If the connection does not exist.
        """
        logger.info("Checking airbyte_default Airflow connection exists")

        conn = Connection.get_connection_from_secrets("airbyte_default")

        host = conn.host
        port = conn.port or 8000
        logger.info(f"airbyte_default found — host: {host}, port: {port}")

    @task(
        retries=3,
        retry_delay=timedelta(seconds=5),
        retry_exponential_backoff=True,
        on_failure_callback=_on_failure,
    )
    def check_airbyte_health():
        """Send a GET request to the Airbyte health endpoint and assert a 200 response.

        Raises:
            Exception: If the HTTP status code is not 200.
        """
        airbyte_host = os.environ.get("AIRBYTE_HOST", "http://host.docker.internal:8000")
        url = f"{airbyte_host}/api/public/v1/health"

        logger.info(f"Starting Airbyte health check at {url}")

        response = requests.get(url)

        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response body: {response.text}")

        if response.status_code != 200:
            raise Exception(f"Airbyte not reachable. Status: {response.status_code}")

        logger.info("Airbyte health check finished successfully")

    check_airflow_connection() >> check_airbyte_health()


airbyte_pre_check_dag()
