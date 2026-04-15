import logging
from datetime import timedelta

from airflow.sdk import dag, task

from db.connection import get_conn
from utils.events import PRE_CHECK_FAILED, emit_event


def _on_failure(context):
    ti = context.get("task_instance")
    emit_event(PRE_CHECK_FAILED, {
        "dag_id": "postgres_landing_pre_check_dag",
        "task_id": ti.task_id if ti else "unknown",
        "exception": str(context.get("exception", "")),
    })


@dag(
    dag_id="postgres_landing_pre_check_dag",
    dag_display_name="Postgres Landing Zone Pre-Check DAG",
    description="Test connection to the PostgreSQL landing zone database",
    schedule=None,
    tags=["pre-check", "postgres", "landing"],
)
def postgres_landing_pre_check_dag():

    logger = logging.getLogger("airflow.pre_checks")

    @task(
        retries=3,
        retry_delay=timedelta(seconds=5),
        retry_exponential_backoff=True,
        on_failure_callback=_on_failure,
    )
    def check_postgres_landing_connection():
        logger.info("Testing connection to PostgreSQL landing zone")

        try:
            conn = get_conn()
            conn.close()
            logger.info("PostgreSQL landing zone connection successful")
        except Exception as e:
            logger.error(f"PostgreSQL landing zone connection failed: {str(e)}")
            raise

    check_postgres_landing_connection()


postgres_landing_pre_check_dag()
