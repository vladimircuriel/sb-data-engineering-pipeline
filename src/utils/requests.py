import logging
import time

MAX_RETRIES = 3
BASE_DELAY = 2


def safe_request(func, ticker: str):
    """Execute a callable with exponential-backoff retries.

    Attempts to call ``func`` up to ``MAX_RETRIES`` times. Between each failed
    attempt the function sleeps for ``BASE_DELAY * 2 ** attempt`` seconds. If
    all attempts are exhausted the last exception is re-raised.

    Args:
        func: A zero-argument callable that performs the request.
        ticker: Ticker symbol used only for warning log messages.

    Returns:
        The value returned by ``func`` on a successful call.

    Raises:
        Exception: Re-raises the exception from the last failed attempt.
    """
    for attempt in range(MAX_RETRIES):
        try:
            return func()
        except Exception as e:
            wait = BASE_DELAY * (2**attempt)
            logging.warning(f"{ticker} retry {attempt + 1}: {e}")
            time.sleep(wait)
            if attempt == MAX_RETRIES - 1:
                raise
