import logging

from airflow.sdk import dag, task, Variable
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor

from db.connection import get_conn
from utils.events import emit_event, PRE_CHECK_FAILED, EXTRACTION_FAILED, DATA_LANDED


def _on_dag_failure(context):
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown"
    emit_event(EXTRACTION_FAILED, {
        "dag_id": "landing_to_clickhouse_sync_dag",
        "task_id": task_id,
        "exception": str(context.get("exception", "")),
    })


def _on_precheck_failure(context):
    ti = context.get("task_instance")
    emit_event(PRE_CHECK_FAILED, {
        "dag_id": "landing_to_clickhouse_sync_dag",
        "task_id": ti.task_id if ti else "unknown",
        "exception": str(context.get("exception", "")),
    })


logger = logging.getLogger("airflow.integration")


@dag(
    dag_id="landing_to_clickhouse_sync_dag",
    dag_display_name="Landing To ClickHouse Sync DAG",
    description="Syncs all landing zone tables to ClickHouse staging via Airbyte",
    schedule=None,
    catchup=False,
    tags=["integration", "airbyte", "clickhouse", "landing"],
    on_failure_callback=_on_dag_failure,
)
def landing_to_clickhouse_sync_dag():

    trigger_airbyte_precheck = TriggerDagRunOperator(
        task_id="trigger_airbyte_precheck",
        trigger_dag_id="airbyte_pre_check_dag",
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

    trigger_clickhouse_precheck = TriggerDagRunOperator(
        task_id="trigger_clickhouse_precheck",
        trigger_dag_id="clickhouse_pre_check_dag",
        wait_for_completion=True,
        poke_interval=10,
        on_failure_callback=_on_precheck_failure,
    )

    sync_landing_to_clickhouse = AirbyteTriggerSyncOperator(
        task_id="sync_landing_to_clickhouse",
        airbyte_conn_id="airbyte_default",
        connection_id="{{ var.value.AIRBYTE_LANDING_TO_CLICKHOUSE_CONNECTION_ID }}",
        asynchronous=True,
        timeout=3600,
        wait_seconds=10,
    )

    wait_for_sync = AirbyteJobSensor(
        task_id="wait_for_sync",
        airbyte_conn_id="airbyte_default",
        airbyte_job_id=sync_landing_to_clickhouse.output,
        timeout=3600,
        poke_interval=30,
    )

    @task
    def update_watermark():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(run_at) FROM yfinance_run_metadata")
                row = cur.fetchone()
                latest = row[0] if row and row[0] else None

        if latest:
            Variable.set("LANDING_LAST_SYNC_AT", str(latest))
            logger.info(f"Watermark updated to {latest}")
            emit_event(DATA_LANDED, {"synced_up_to": str(latest)})

    (
        [trigger_airbyte_precheck, trigger_postgres_precheck, trigger_clickhouse_precheck]
        >> sync_landing_to_clickhouse
        >> wait_for_sync
        >> update_watermark()
    )


landing_to_clickhouse_sync_dag()
