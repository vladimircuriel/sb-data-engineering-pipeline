import logging

logger = logging.getLogger("airflow.metrics")

CATEGORIES = ("prices", "fundamentals", "holders", "recommendations")


def compute_ingestion_metrics(consolidated: list[dict]) -> dict:
    """Compute summary metrics from the consolidated extraction results.

    For each ticker the function counts rows extracted per category.  A ticker
    is considered *complete* when it has at least one row in every category.

    Args:
        consolidated: List of per-ticker dicts as produced by the
            ``consolidate_data`` task.  Each dict must contain the keys
            ``ticker``, ``prices``, ``fundamentals``, ``holders``, and
            ``recommendations``.

    Returns:
        A dict with the following keys:

        * ``total_tickers`` (int): Total number of tickers processed.
        * ``total_price_rows`` (int): Sum of price rows across all tickers.
        * ``tickers_with_no_data`` (list[str]): Tickers with zero rows in
          every category.
        * ``rows_per_ticker`` (dict[str, dict[str, int]]): Per-category row
          counts keyed by ticker symbol.
        * ``completeness_pct`` (float): Percentage of tickers that are
          complete, rounded to two decimal places.
    """
    total_tickers = len(consolidated)
    rows_per_ticker: dict[str, dict[str, int]] = {}
    tickers_with_no_data: list[str] = []
    total_price_rows = 0
    complete_count = 0

    for row in consolidated:
        ticker = row["ticker"]
        counts = {cat: len(row.get(cat) or []) for cat in CATEGORIES}
        rows_per_ticker[ticker] = counts
        total_price_rows += counts["prices"]

        if all(counts[cat] > 0 for cat in CATEGORIES):
            complete_count += 1
        if all(counts[cat] == 0 for cat in CATEGORIES):
            tickers_with_no_data.append(ticker)

    completeness_pct = (complete_count / total_tickers * 100) if total_tickers else 0.0

    metrics = {
        "total_tickers": total_tickers,
        "total_price_rows": total_price_rows,
        "tickers_with_no_data": tickers_with_no_data,
        "rows_per_ticker": rows_per_ticker,
        "completeness_pct": round(completeness_pct, 2),
    }

    logger.info("Ingestion metrics: %d tickers, %d price rows, %.1f%% complete",
                total_tickers, total_price_rows, completeness_pct)
    return metrics
