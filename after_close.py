import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz

from bot_config import (
    require_env, DATA_DIR,
    HOLD_DAYS, BOTTOM_N, ZSCORE_WINDOW, RSI_PERIOD, ZSCORE_FILTER,
    DISP_LAG_DAYS, LOW_Q, ROLLING_WINDOW, MIN_NAMES_FOR_DISP, MIN_PRICE,
    USE_LOW_VOL_SKIP, REGIME_SYMBOL, REGIME_VOL_WINDOW, LOW_VOL_SKIP_PCT,
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
    Return (last_good_date, coverage_series, last_date, last_cov)
    last_good_date = most recent date with >= min_coverage unique symbols.
    """
    coverage = bars.groupby("date")["symbol"].nunique().sort_index()
    last_date = coverage.index.max()
    last_cov = int(coverage.loc[last_date]) if last_date is not None else 0

    ok = coverage[coverage >= min_coverage]
    last_good = ok.index.max() if not ok.empty else None

    return last_good, coverage, last_date, last_cov


def compute_low_vol_gate(
    bars_all: pd.DataFrame,
    signal_date,
    regime_symbol: str,
    vol_window: int,
    low_vol_skip_pct: float,
):
    """
    Compute whether we should ALLOW trading on signal_date based on whether
    SPY 20d realized vol is above the bottom X percentile.

    Returns:
      regime_ok, current_vol, cutoff, spy_regime_df
    """
    spy = bars_all[bars_all["symbol"] == regime_symbol].copy()
    if spy.empty:
        raise RuntimeError(
            f"Regime symbol {regime_symbol} not found in returned bars. "
            f"Make sure it is included in the data pull."
        )

    spy = spy.sort_values("date").copy()
    spy["spy_ret"] = spy["close"].pct_change()
    spy["vol20"] = spy["spy_ret"].rolling(vol_window).std()

    hist = spy.loc[spy["date"] <= signal_date, ["date", "vol20"]].copy()
    hist = hist.dropna(subset=["vol20"]).copy()

    if hist.empty:
        return False, np.nan, np.nan, spy

    cutoff = hist["vol20"].quantile(low_vol_skip_pct)

    row = hist[hist["date"] == signal_date]
    if row.empty:
        return False, np.nan, cutoff, spy

    current_vol = float(row["vol20"].iloc[0])
    regime_ok = bool(current_vol > cutoff)

    return regime_ok, current_vol, float(cutoff), spy


def main():
    require_env()
    init_db()
    ensure_dir()

    now_et = datetime.now(ET)
    today = now_et.date()

    cal = get_trading_calendar(start=str(today - timedelta(days=10)), end=str(today + timedelta(days=30)))
    if cal.empty:
        raise RuntimeError("Trading calendar empty; check Alpaca connectivity.")

    # Guard: cron may run on weekends/holidays. Only generate plans on trading days.
    cal_dates = set(cal["date"].tolist())
    if today not in cal_dates:
        print(f"Not a trading day ({today}); exiting.")
        return

    next_td = get_next_trading_day(cal, today_date=today)

    universe_path = os.path.join(DATA_DIR, "universe.csv")
    if os.path.exists(universe_path):
        symbols = pd.read_csv(universe_path)["symbol"].dropna().astype(str).str.upper().unique().tolist()
    else:
        raise RuntimeError(
            f"Missing {universe_path}. Create it with a 'symbol' column of tickers (your universe list)."
        )

    # Always fetch regime symbol too, even if not in tradable universe
    fetch_symbols = sorted(set(symbols + [REGIME_SYMBOL]))

    # Pull enough history for rolling regime + indicators
    start = (today - timedelta(days=600)).isoformat()
    end = (today + timedelta(days=1)).isoformat()

    bars = get_daily_bars(fetch_symbols, start=start, end=end)
    if bars.empty:
        raise RuntimeError("No bars returned. Check symbols, market data permissions, and dates.")

    bars["symbol"] = bars["symbol"].astype(str).str.upper()

    # -------------------------
    # Diagnostics: coverage
    # -------------------------
    # Coverage should be based on actual tradable universe, not including SPY helper symbol
    bars_universe = bars[bars["symbol"].isin(symbols)].copy()

    n_syms_returned = bars_universe["symbol"].nunique()
    n_rows = len(bars_universe)

    MIN_COVERAGE = min(150, max(30, int(0.6 * len(symbols))))
    last_good_date, coverage, last_date, last_cov = pick_last_good_date(bars_universe, min_coverage=MIN_COVERAGE)

    cov_max = int(coverage.max()) if len(coverage) else 0
    cov_min = int(coverage.min()) if len(coverage) else 0

    print("=== Data Diagnostics ===")
    print(f"Requested tradable symbols: {len(symbols)}")
    print(f"Fetched symbols total:      {bars['symbol'].nunique()} (includes regime symbol {REGIME_SYMBOL})")
    print(f"Returned tradable symbols:  {n_syms_returned}")
    print(f"Total tradable bar rows:    {n_rows}")
    print(f"Latest tradable date:       {last_date} | coverage={last_cov}")
    print(f"Max coverage:               {cov_max} | Min coverage: {cov_min}")
    print(f"Coverage threshold:         {MIN_COVERAGE}")
    print(f"Using last_good_date:       {last_good_date}")
    print("========================\n")

    if last_good_date is None:
        raise RuntimeError(
            f"No date met MIN_COVERAGE={MIN_COVERAGE}. "
            f"Best coverage was {cov_max}. "
            "This usually means IEX feed returned partial data. "
            "Either reduce MIN_COVERAGE or switch data feed/source."
        )

    # -------------------------
    # Use last_good_date for price filter & signals
    # -------------------------
    latest = (
        bars_universe[bars_universe["date"] == last_good_date][["symbol", "close"]]
        .rename(columns={"close": "last_close"})
    )
    tradable = latest[latest["last_close"] >= MIN_PRICE]["symbol"].tolist()

    # Keep only tradable symbols for indicator + dispersion computation
    df = bars_universe[bars_universe["symbol"].isin(tradable)].copy()

    # Indicators on close
    df = add_indicators(df, z_window=ZSCORE_WINDOW, rsi_period=RSI_PERIOD)
    z_col = f"zscore_{ZSCORE_WINDOW}"
    rsi_col = f"rsi_{RSI_PERIOD}"

    # Dispersion gate on tradable set
    disp = compute_dispersion(df)  # index=date, cols: disp, n
    disp = disp[disp["n"] >= MIN_NAMES_FOR_DISP].copy()
    disp["disp_lag"] = disp["disp"].shift(DISP_LAG_DAYS)
    disp["thr"] = disp["disp_lag"].rolling(ROLLING_WINDOW).quantile(LOW_Q).shift(1)
    disp["gate_ok"] = (disp["disp_lag"] >= disp["thr"]) & disp["thr"].notna()

    if last_good_date not in disp.index:
        disp_gate_ok = False
        disp_lag = np.nan
        disp_thr = np.nan
    else:
        disp_gate_ok = bool(disp.loc[last_good_date, "gate_ok"]) if pd.notna(disp.loc[last_good_date, "gate_ok"]) else False
        disp_lag = float(disp.loc[last_good_date, "disp_lag"]) if pd.notna(disp.loc[last_good_date, "disp_lag"]) else np.nan
        disp_thr = float(disp.loc[last_good_date, "thr"]) if pd.notna(disp.loc[last_good_date, "thr"]) else np.nan

    # Low-vol regime gate from SPY
    if USE_LOW_VOL_SKIP:
        regime_ok, spy_vol20, spy_cutoff, _ = compute_low_vol_gate(
            bars_all=bars,
            signal_date=last_good_date,
            regime_symbol=REGIME_SYMBOL,
            vol_window=REGIME_VOL_WINDOW,
            low_vol_skip_pct=LOW_VOL_SKIP_PCT,
        )
    else:
        regime_ok = True
        spy_vol20 = np.nan
        spy_cutoff = np.nan

    # Final gate combines both
    gate_ok = bool(disp_gate_ok and regime_ok)

    # Build buy list using same last_good_date
    day = df[df["date"] == last_good_date].dropna(subset=[z_col, rsi_col]).copy()
    day = day[day[z_col] <= ZSCORE_FILTER].copy()
    day = day.sort_values(rsi_col, ascending=True)

    buy_symbols = day["symbol"].head(BOTTOM_N).tolist() if gate_ok else []

    upsert_plan(plan_date=next_td, gate_ok=gate_ok, buy_symbols=buy_symbols)

    msg = [
        "📊 After-Close Plan Created",
        f"Signal date used: {last_good_date}",
        f"Plan date (next open): {next_td}",
        f"Dispersion gate: {'ON ✅' if disp_gate_ok else 'OFF ⛔'}",
        f"  disp_lag={disp_lag:.6f}" if pd.notna(disp_lag) else "  disp_lag=NaN",
        f"  threshold={disp_thr:.6f}" if pd.notna(disp_thr) else "  threshold=NaN",
    ]

    if USE_LOW_VOL_SKIP:
        msg += [
            f"Low-vol skip gate ({REGIME_SYMBOL} vol{REGIME_VOL_WINDOW}, skip lowest {int(LOW_VOL_SKIP_PCT*100)}%): {'ON ✅' if regime_ok else 'OFF ⛔'}",
            f"  current_vol={spy_vol20:.6f}" if pd.notna(spy_vol20) else "  current_vol=NaN",
            f"  cutoff={spy_cutoff:.6f}" if pd.notna(spy_cutoff) else "  cutoff=NaN",
        ]

    msg += [
        f"Final gate: {'ON ✅' if gate_ok else 'OFF ⛔'}",
        f"Universe tradable (price>=${MIN_PRICE}): {len(tradable)} (of {len(symbols)} requested)",
        f"Candidate pool (z<={ZSCORE_FILTER}): {len(day)}",
        f"Buys tomorrow ({len(buy_symbols)}): {', '.join(buy_symbols) if buy_symbols else 'None'}",
    ]

    tg_send("\n".join(msg))
    log_event("AFTER_CLOSE", " | ".join(msg))
    print("\n".join(msg))


if __name__ == "__main__":
    main()