import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz

from bot_config import (
    require_env, DATA_DIR,
    HOLD_DAYS, BOTTOM_N, ZSCORE_WINDOW, RSI_PERIOD, ZSCORE_FILTER,
    DISP_LAG_DAYS, LOW_Q, ROLLING_WINDOW, MIN_NAMES_FOR_DISP, MIN_PRICE,
)
from alpaca_utils import get_trading_calendar, get_next_trading_day, get_daily_bars
from indicators import add_indicators, compute_dispersion
from state_db import init_db, upsert_plan, log_event
from telegram_utils import tg_send

ET = pytz.timezone("America/New_York")


def ensure_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def pick_last_good_date(bars: pd.DataFrame, min_coverage: int) -> tuple:
    """
    Return (last_good_date, coverage_series)
    last_good_date = most recent date with >= min_coverage unique symbols.
    """
    coverage = bars.groupby("date")["symbol"].nunique().sort_index()
    last_date = coverage.index.max()
    last_cov = int(coverage.loc[last_date]) if last_date is not None else 0

    # choose most recent date with enough symbol coverage
    ok = coverage[coverage >= min_coverage]
    last_good = ok.index.max() if not ok.empty else None

    return last_good, coverage, last_date, last_cov


def main():
    require_env()
    init_db()
    ensure_dir()

    now_et = datetime.now(ET)
    today = now_et.date()

    cal = get_trading_calendar(start=str(today - timedelta(days=10)), end=str(today + timedelta(days=30)))
    if cal.empty:
        raise RuntimeError("Trading calendar empty; check Alpaca connectivity.")

    next_td = get_next_trading_day(cal, today_date=today)

    universe_path = os.path.join(DATA_DIR, "universe.csv")
    if os.path.exists(universe_path):
        symbols = pd.read_csv(universe_path)["symbol"].dropna().unique().tolist()
    else:
        raise RuntimeError(
            f"Missing {universe_path}. Create it with a 'symbol' column of tickers (your 244 list)."
        )

    # Pull enough history for rolling regime + indicators
    start = (today - timedelta(days=600)).isoformat()
    end = (today + timedelta(days=1)).isoformat()

    bars = get_daily_bars(symbols, start=start, end=end)
    if bars.empty:
        raise RuntimeError("No bars returned. Check symbols, market data permissions, and dates.")

    # -------------------------
    # Diagnostics: coverage
    # -------------------------
    n_syms_returned = bars["symbol"].nunique()
    n_rows = len(bars)

    # We expect close to 244 symbols on most recent "good" day.
    # IEX can be patchy, so set a reasonable minimum.
    MIN_COVERAGE = min(150, max(30, int(0.6 * len(symbols))))  # ~60% of your universe, capped
    last_good_date, coverage, last_date, last_cov = pick_last_good_date(bars, min_coverage=MIN_COVERAGE)

    cov_max = int(coverage.max()) if len(coverage) else 0
    cov_min = int(coverage.min()) if len(coverage) else 0

    print("=== Data Diagnostics ===")
    print(f"Requested symbols: {len(symbols)}")
    print(f"Returned symbols:  {n_syms_returned}")
    print(f"Total bar rows:    {n_rows}")
    print(f"Latest date:       {last_date} | coverage={last_cov}")
    print(f"Max coverage:      {cov_max} | Min coverage: {cov_min}")
    print(f"Coverage threshold (MIN_COVERAGE): {MIN_COVERAGE}")
    print(f"Using last_good_date: {last_good_date}")
    print("========================\n")

    if last_good_date is None:
        # If Alpaca data is too patchy, we should not proceed with trading signals.
        raise RuntimeError(
            f"No date met MIN_COVERAGE={MIN_COVERAGE}. "
            f"Best coverage was {cov_max}. "
            "This usually means IEX feed returned partial data. "
            "Either reduce MIN_COVERAGE or switch data feed/source."
        )

    # -------------------------
    # Use last_good_date for price filter & signals
    # -------------------------
    latest = bars[bars["date"] == last_good_date][["symbol", "close"]].rename(columns={"close": "last_close"})
    tradable = latest[latest["last_close"] >= MIN_PRICE]["symbol"].tolist()

    # keep only tradable symbols for indicator + dispersion computation
    df = bars[bars["symbol"].isin(tradable)].copy()

    # Indicators on close
    df = add_indicators(df, z_window=ZSCORE_WINDOW, rsi_period=RSI_PERIOD)
    z_col = f"zscore_{ZSCORE_WINDOW}"
    rsi_col = f"rsi_{RSI_PERIOD}"

    # Dispersion (computed on tradable set)
    disp = compute_dispersion(df)  # columns: disp, n
    disp = disp[disp["n"] >= MIN_NAMES_FOR_DISP].copy()
    disp["disp_lag"] = disp["disp"].shift(DISP_LAG_DAYS)

    disp["thr"] = disp["disp_lag"].rolling(ROLLING_WINDOW).quantile(LOW_Q).shift(1)
    disp["gate_ok"] = (disp["disp_lag"] >= disp["thr"]) & disp["thr"].notna()

    last_disp_date = disp.dropna(subset=["gate_ok"]).index.max()
    gate_ok = bool(disp.loc[last_disp_date, "gate_ok"]) if pd.notna(last_disp_date) else False

    # Build buy list using the same last_good_date (t-1 in “live” is handled by scheduling)
    # In a true live setup, after close on date D, you trade at open on D+1.
    # So signals should be computed off date D’s close (this last_good_date should represent that).
    day = df[df["date"] == last_good_date].dropna(subset=[z_col, rsi_col]).copy()
    day = day[day[z_col] <= ZSCORE_FILTER].copy()
    day = day.sort_values(rsi_col, ascending=True)

    buy_symbols = day["symbol"].head(BOTTOM_N).tolist() if gate_ok else []

    upsert_plan(plan_date=next_td, gate_ok=gate_ok, buy_symbols=buy_symbols)

    msg = [
        "📊 After-Close Plan Created",
        f"Signal date used: {last_good_date}",
        f"Plan date (next open): {next_td}",
        f"Gate (disp rolling q{int(LOW_Q*100)} / {ROLLING_WINDOW}d): {'ON ✅' if gate_ok else 'OFF ⛔'}",
        f"Universe tradable (price>=${MIN_PRICE}): {len(tradable)} (of {len(symbols)} requested)",
        f"Candidate pool (z<={ZSCORE_FILTER}): {len(day)}",
        f"Buys tomorrow ({len(buy_symbols)}): {', '.join(buy_symbols) if buy_symbols else 'None'}",
    ]
    tg_send("\n".join(msg))
    log_event("AFTER_CLOSE", " | ".join(msg))
    print("\n".join(msg))


if __name__ == "__main__":
    main()