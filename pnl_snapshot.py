from datetime import datetime
import pytz
import pandas as pd

from bot_config import require_env
from alpaca_utils import get_account, list_open_positions
from state_db import init_db, open_lots, upsert_equity_snapshot, log_event
from telegram_utils import tg_send

ET = pytz.timezone("America/New_York")


def _to_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return float(default)


def main():
    require_env()
    init_db()

    now_et = datetime.now(ET)
    snap_date = str(now_et.date())

    acct = get_account()
    equity = _to_float(acct.get("equity"))
    cash = _to_float(acct.get("cash"))
    buying_power = _to_float(acct.get("buying_power"))

    pos = list_open_positions()
    if pos is None or pos.empty:
        pos = pd.DataFrame([])

    lots = open_lots(include_pending_entry=True)
    bot_syms = set(lots["symbol"].unique().tolist()) if not lots.empty else set()

    bot_pos = pos[pos["symbol"].isin(bot_syms)].copy() if (not pos.empty and bot_syms) else pd.DataFrame([])

    bot_mv = 0.0
    bot_upl = 0.0
    if not bot_pos.empty:
        bot_mv = bot_pos["market_value"].apply(_to_float).sum()
        bot_upl = bot_pos["unrealized_pl"].apply(_to_float).sum()

    upsert_equity_snapshot(
        snap_date=snap_date,
        equity=equity,
        cash=cash,
        buying_power=buying_power,
        bot_mv=bot_mv,
        bot_unrealized_pl=bot_upl,
        note="EOD snapshot",
    )

    msg = [
        "📌 EOD PnL Snapshot",
        f"Date (ET): {snap_date}",
        "",
        f"Account equity: ${equity:,.2f}",
        f"Cash:          ${cash:,.2f}",
        f"Buying power:  ${buying_power:,.2f}",
        "",
        f"Bot symbols (OPEN/PENDING lots): {len(bot_syms)}",
        f"Bot market value:               ${bot_mv:,.2f}",
        f"Bot unrealized P/L:             ${bot_upl:,.2f}",
    ]

    tg_send("\n".join(msg))
    log_event("PNL_SNAPSHOT", " | ".join(msg))
    print("\n".join(msg))


if __name__ == "__main__":
    main()