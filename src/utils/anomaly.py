import logging

from config.tickers import BANK_TICKERS

logger = logging.getLogger("airflow.anomaly")

DROP_THRESHOLD_PCT = 50


def detect_volume_anomalies(metrics: dict, prev_rows_inserted: int | None) -> list[dict]:
    """Analyse ingestion metrics and return a list of detected anomalies.

    Three checks are performed:

    * **Missing tickers** – tickers present in ``BANK_TICKERS`` but absent from
      the current run's ``rows_per_ticker`` map.
    * **Tickers with no data** – tickers that returned zero rows in every
      category (prices, fundamentals, holders, recommendations).
    * **Volume drop** – total price rows fell by ``DROP_THRESHOLD_PCT`` percent
      or more compared to the previous run.

    Args:
        metrics: Output of :func:`~utils.metrics.compute_ingestion_metrics`,
            expected keys: ``rows_per_ticker``, ``tickers_with_no_data``,
            ``total_price_rows``.
        prev_rows_inserted: Price-row count from the most recent previous run,
            or ``None`` if no prior run exists.

    Returns:
        A list of anomaly dicts.  Each dict contains at minimum ``type`` and
        ``detail`` keys; additional keys vary by anomaly type.
    """
    anomalies: list[dict] = []

    # Check for missing tickers
    extracted = set(metrics["rows_per_ticker"].keys())
    expected = set(BANK_TICKERS)
    missing = expected - extracted
    if missing:
        anomalies.append({
            "type": "missing_tickers",
            "detail": f"Missing tickers: {sorted(missing)}",
            "missing": sorted(missing),
        })

    # Check for tickers with zero data across all categories
    for ticker in metrics.get("tickers_with_no_data", []):
        anomalies.append({
            "type": "ticker_no_data",
            "detail": f"{ticker} returned zero rows in all categories",
            "ticker": ticker,
        })

    # Check total price rows vs previous run
    if prev_rows_inserted and prev_rows_inserted > 0:
        current = metrics["total_price_rows"]
        drop_pct = (1 - current / prev_rows_inserted) * 100
        if drop_pct >= DROP_THRESHOLD_PCT:
            anomalies.append({
                "type": "volume_drop",
                "detail": f"Price rows dropped {drop_pct:.0f}% vs previous run "
                          f"({current} vs {prev_rows_inserted})",
                "current": current,
                "previous": prev_rows_inserted,
                "drop_pct": round(drop_pct, 1),
            })

    if anomalies:
        logger.warning("Volume anomalies detected: %d issue(s)", len(anomalies))
        for a in anomalies:
            logger.warning("  - %s", a["detail"])
    else:
        logger.info("No volume anomalies detected")

    return anomalies
