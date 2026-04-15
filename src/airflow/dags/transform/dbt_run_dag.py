import os
import logging

from airflow.sdk import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from docker.types import Mount

from utils.events import emit_event, TRANSFORM_COMPLETED, TRANSFORM_FAILED


def _on_failure(context):
    ti = context.get("task_instance")
    emit_event(TRANSFORM_FAILED, {
        "dag_id": "dbt_run_dag",
        "task_id": ti.task_id if ti else "unknown",
        "exception": str(context.get("exception", "")),
    })


logger = logging.getLogger("airflow.transform")


@dag(
    dag_id="dbt_run_dag",
    dag_display_name="dbt Run DAG",
    description="Transforms ClickHouse staging data into dwh using dbt",
    schedule=None,
    catchup=False,
    tags=["transform", "dbt", "clickhouse"],
    on_failure_callback=_on_failure,
)
def dbt_run_dag():

    trigger_dbt_precheck = TriggerDagRunOperator(
        task_id="trigger_dbt_precheck",
        trigger_dag_id="dbt_pre_check_dag",
        wait_for_completion=True,
        poke_interval=10,
        on_failure_callback=_on_failure,
    )

    host_proj_dir = os.environ.get("HOST_PROJ_DIR", "")
    compose_project = os.environ.get("COMPOSE_PROJECT_NAME", "sb_pipeline")

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image=f"{compose_project}-dbt",
        command="dbt run",
        docker_url="unix://var/run/docker.sock",
        network_mode=f"{compose_project}_default",
        environment={
            "CLICKHOUSE_HOST":      os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            "CLICKHOUSE_HTTP_PORT": os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"),
            "CLICKHOUSE_USER":      os.environ.get("CLICKHOUSE_USER", "admin"),
            "CLICKHOUSE_PASSWORD":  os.environ.get("CLICKHOUSE_PASSWORD", "admin"),
            "CLICKHOUSE_DB":        os.environ.get("CLICKHOUSE_DB", "dwh"),
        },
        mounts=[
            Mount(
                source=f"{host_proj_dir}/src/dbt",
                target="/dbt",
                type="bind",
            )
        ],
        mount_tmp_dir=False,
        auto_remove="success",
        force_pull=False,
        on_failure_callback=_on_failure,
    )

    @task
    def notify_completed():
        logger.info("dbt run completed successfully")
        emit_event(TRANSFORM_COMPLETED, {"dag_id": "dbt_run_dag"})

    trigger_dbt_precheck >> dbt_run >> notify_completed()


dbt_run_dag()
