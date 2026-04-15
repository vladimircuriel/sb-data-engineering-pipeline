import logging

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable

from db.connection import get_conn
from utils.events import NO_NEW_DATA, SYNC_TRIGGERED, emit_event


@dag(
    dag_id="landing_zone_polling_dag",
    dag_display_name="Landing Zone Polling DAG",
    description="Polls the landing zone for new data and triggers the ClickHouse sync if detected",
    schedule="*/15 * * * *",
    catchup=False,
    tags=["polling", "landing", "clickhouse"],
)
def landing_zone_polling_dag():

    logger = logging.getLogger("airflow.polling")

    @task.short_circuit
    def has_new_data():
        last_sync_at = Variable.get("LANDING_LAST_SYNC_AT", default=None)
        logger.info(f"Last sync watermark: {last_sync_at}")

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.tables
                        WHERE table_name = 'yfinance_run_metadata'
                    )
                """)
                exists = cur.fetchone()
                if not exists or not exists[0]:
                    logger.info("yfinance_run_metadata table does not exist yet")
                    emit_event(NO_NEW_DATA, {"reason": "table_not_found"})
                    return False

                if last_sync_at:
                    cur.execute("""
                        SELECT COUNT(*) FROM yfinance_run_metadata
                        WHERE run_at > %s
                    """, (last_sync_at,))
                else:
                    cur.execute("SELECT COUNT(*) FROM yfinance_run_metadata")

                row = cur.fetchone()
                new_runs = row[0] if row else 0

        if new_runs == 0:
            logger.info("No new landing runs detected since last sync")
            emit_event(NO_NEW_DATA, {"reason": "no_new_runs", "last_sync_at": last_sync_at})
            return False

        logger.info(f"Detected {new_runs} new landing run(s) since last sync — triggering sync")
        emit_event(SYNC_TRIGGERED, {"dag_id": "landing_to_clickhouse_sync_dag", "new_runs": new_runs, "last_sync_at": last_sync_at})
        return True

    trigger_sync = TriggerDagRunOperator(
        task_id="trigger_landing_to_clickhouse_sync",
        trigger_dag_id="landing_to_clickhouse_sync_dag",
        wait_for_completion=False,
    )

    has_new_data() >> trigger_sync


landing_zone_polling_dag()
