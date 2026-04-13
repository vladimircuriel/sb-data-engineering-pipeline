import os
import requests
import logging

from datetime import timedelta
from airflow.sdk import dag, task


@dag(
    dag_id="airbyte_pre_check_dag",
    dag_display_name="Airbyte Sync DAG",
    description="A DAG to trigger Airbyte sync and print the status",
    schedule=None,
    tags=["pre-check", "test", "airbyte"],
)
def airbyte_pre_check_dag():
    """ """
    logger = logging.getLogger("airflow.pre_checks")

    @task(
        retries=3,
        retry_delay=timedelta(seconds=5),
        retry_exponential_backoff=True,
    )
    def check_airbyte_health():
        airbyte_host = os.environ.get(
            "AIRBYTE_HOST", "http://host.docker.internal:8000"
        )
        url = f"{airbyte_host}/api/public/v1/health"

        logger.info(f"Starting Airbyte health check at {url}")

        response = requests.get(url)

        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response body: {response.text}")

        if response.status_code == 200:
            logger.info("Airbyte health check finished successfully")
        else:
            logger.error(f"Airbyte not reachable. Status: {response.status_code}")
            raise Exception(f"Airbyte not reachable. Status: {response.status_code}")

        return response.json()

    check_airbyte_health()


airbyte_pre_check_dag()
