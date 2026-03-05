import numpy as np
import pandas as pd
from ta.momentum import RSIIndicator

def add_indicators(df: pd.DataFrame, z_window=5, rsi_period=2) -> pd.DataFrame:
    """
    df expected columns: date, symbol, close
    Adds: zscore_5, rsi_2
    """
    df = df.copy()
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    g = df.groupby("symbol", group_keys=False)

    # zscore on close: (close - mean)/std rolling
    roll_mean = g["close"].rolling(z_window).mean().reset_index(level=0, drop=True)
    roll_std = g["close"].rolling(z_window).std(ddof=0).reset_index(level=0, drop=True)
    df[f"zscore_{z_window}"] = (df["close"] - roll_mean) / roll_std

    # RSI
    def rsi_apply(x: pd.Series) -> pd.Series:
        return RSIIndicator(close=x, window=rsi_period).rsi()

    df[f"rsi_{rsi_period}"] = g["close"].apply(rsi_apply).reset_index(level=0, drop=True)
    return df

def compute_dispersion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dispersion = cross-sectional std of ret1 each date
    df must contain date, symbol, close
    """
    df = df.copy()
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    g = df.groupby("symbol", group_keys=False)
    df["ret1"] = g["close"].pct_change()

    disp = df.groupby("date")["ret1"].agg(["std", "count"]).rename(columns={"std": "disp", "count": "n"})
    return disp