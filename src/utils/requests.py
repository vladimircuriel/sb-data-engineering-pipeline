import logging
import time

MAX_RETRIES = 3
BASE_DELAY = 2


def safe_request(func, ticker: str):
    for attempt in range(MAX_RETRIES):
        try:
            return func()
        except Exception as e:
            wait = BASE_DELAY * (2**attempt)
            logging.warning(f"{ticker} retry {attempt + 1}: {e}")
            time.sleep(wait)
            if attempt == MAX_RETRIES - 1:
                raise
