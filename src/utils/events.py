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
TRANSFORM_COMPLETED = "TRANSFORM_COMPLETED"
TRANSFORM_FAILED = "TRANSFORM_FAILED"
SYNC_TRIGGERED = "SYNC_TRIGGERED"


def emit_event(event_type: str, context: dict | None = None) -> dict:
    """Emit a pipeline event to the log and persist it in PostgreSQL.

    The event is always written to the Airflow logger regardless of whether the
    database write succeeds.  A failed database write is logged as an exception
    but does not propagate so that pipeline tasks are never blocked by event
    persistence errors.

    Args:
        event_type: One of the event-type constants defined in this module
            (e.g. ``DATA_LANDED``, ``EXTRACTION_FAILED``).
        context: Optional dict with additional metadata to store alongside the
            event.  Defaults to an empty dict when ``None``.

    Returns:
        The event dict that was built, containing ``event_type``, ``timestamp``
        (ISO-8601 UTC), and ``context``.
    """
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
