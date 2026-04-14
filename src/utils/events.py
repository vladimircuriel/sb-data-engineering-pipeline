import json
import logging
from datetime import datetime, timezone

from db.connection import get_conn
from db.landing import create_tables, insert_event

logger = logging.getLogger("airflow.events")

PRE_CHECK_FAILED = "PRE_CHECK_FAILED"
EXTRACT_ERROR = "EXTRACT_ERROR"
VOLUME_ANOMALY = "VOLUME_ANOMALY"
EXTRACTION_FAILED = "EXTRACTION_FAILED"
NO_NEW_DATA = "NO_NEW_DATA"
DATA_LANDED = "DATA_LANDED"


def emit_event(event_type: str, context: dict | None = None) -> dict:
    event = {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "context": context or {},
    }
    logger.warning("PIPELINE_EVENT | %s", json.dumps(event))

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                create_tables(cur)
                insert_event(cur, event_type, context)
            conn.commit()
    except Exception:
        logger.exception("Failed to persist event to database")

    return event
