import pandas as pd


def validate_df(df: pd.DataFrame, name: str, ticker: str):
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        raise ValueError(f"{ticker} -> {name} invalid")
