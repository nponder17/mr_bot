import sqlite3
from datetime import datetime
import pytz

import pandas as pd

from bot_config import DB_PATH, require_env
from alpaca_utils import list_open_positions
from state_db import init_db

ET = pytz.timezone("America/New_York")


def _to_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return float(default)


def main():
    require_env()
    init_db()

    # Pull broker positions
    pos = list_open_positions()
    if pos is None or pos.empty:
        print("No open broker positions found.")
        return

    pos = pos.copy()
    pos["qty"] = pos["qty"].apply(_to_float)
    pos["avg_entry_price"] = pos["avg_entry_price"].apply(_to_float)

    # Pull OPEN lots that have no fill info yet (legacy)
    with sqlite3.connect(DB_PATH) as conn:
        lots = pd.read_sql_query(
            """
            SELECT lot_id, symbol, entry_date, exit_date, notional, status,
                   entry_order_id, entry_client_order_id, qty, avg_entry_price, filled_notional_entry
            FROM lots
            WHERE status='OPEN'
            """,
            conn,
        )

    if lots.empty:
        print("No OPEN lots found in DB.")
        return

    # Only reconcile lots that are missing fill fields
    legacy = lots[
        lots["qty"].isna() | lots["avg_entry_price"].isna() | lots["entry_order_id"].isna()
    ].copy()

    if legacy.empty:
        print("No legacy lots to reconcile (all lots already have fill fields).")
        return

    # Map broker positions by symbol
    pos_map = {r["symbol"]: r for _, r in pos.iterrows()}

    now_iso = datetime.now(ET).isoformat()
    updates = []

    for sym, g in legacy.groupby("symbol"):
        if sym not in pos_map:
            print(f"⚠️ DB has lots for {sym}, but broker has no position. Skipping.")
            continue

        broker_qty = float(pos_map[sym]["qty"])
        broker_avg = float(pos_map[sym]["avg_entry_price"])

        if broker_qty <= 0 or broker_avg <= 0:
            print(f"⚠️ Broker qty/avg invalid for {sym}: qty={broker_qty} avg={broker_avg}. Skipping.")
            continue

        total_notional = float(g["notional"].sum())
        if total_notional <= 0:
            total_notional = 1.0

        # Allocate qty across lots pro-rata by notional
        # (so overlap symbols split ~50/50 between the two $200 lots)
        for _, lot in g.iterrows():
            lot_id = int(lot["lot_id"])
            w = float(lot["notional"]) / total_notional
            alloc_qty = broker_qty * w
            alloc_notional = alloc_qty * broker_avg

            updates.append((alloc_qty, broker_avg, alloc_notional, now_iso, lot_id))

    if not updates:
        print("No updates to apply.")
        return

    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            """
            UPDATE lots
            SET
                qty=?,
                avg_entry_price=?,
                filled_notional_entry=?,
                entry_filled_at=?,
                entry_order_id=COALESCE(entry_order_id, 'RECONCILED'),
                entry_client_order_id=COALESCE(entry_client_order_id, 'RECONCILED')
            WHERE lot_id=?
            """,
            updates,
        )
        conn.commit()

    print(f"✅ Reconciled {len(updates)} legacy lots.")
    print("Next: run the verification SQL commands shown in the instructions.")


if __name__ == "__main__":
    main()