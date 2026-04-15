import pandas as pd


def validate_df(df: pd.DataFrame, name: str, ticker: str):
    """Raise a ValueError if the given DataFrame is None, not a DataFrame, or empty.

    Args:
        df: The DataFrame to validate.
        name: A label describing the dataset (used in the error message).
        ticker: The ticker symbol associated with the data (used in the error message).

    Raises:
        ValueError: If ``df`` is None, not a :class:`pandas.DataFrame`, or empty.
    """
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        raise ValueError(f"{ticker} -> {name} invalid")
