from datetime import datetime, timedelta
import pytz

from bot_config import (
    require_env, HOLD_DAYS,
    NOTIONAL_PER_POSITION
)
from alpaca_utils import (
    get_trading_calendar, add_trading_days,
    submit_market_order, get_position, wait_for_order_terminal,
    get_order, get_order_by_client_order_id
)
from state_db import (
    init_db, get_plan, plan_already_executed, mark_plan_executed,
    lots_exiting_on, lot_exists_for_entry,
    log_event,
    add_lot_pending_entry, mark_lot_open_filled, mark_lot_failed,
    mark_lots_pending_exit, close_lots_for_symbol_exitdate_filled,
    get_open_lots_for_symbol_exitdate,
    get_pending_entries, get_pending_exits, reopen_pending_exit
)
from telegram_utils import tg_send

ET = pytz.timezone("America/New_York")

# Live controls
DRY_RUN = True
FORCE_EXEC_DATE = 2026-03-06  # e.g. "2026-03-05" to test exits early

# Avoid tiny "dust" sells
MIN_SELL_QTY = 1e-6

# Order fill wait settings
FILL_TIMEOUT_SEC = 75
FILL_POLL_SEC = 1.5


def _safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return float(default)


def _order_terminal_summary(o: dict) -> str:
    st = (o.get("status") or "").lower()
    oid = o.get("id")
    coid = o.get("client_order_id")
    fq = o.get("filled_qty")
    fap = o.get("filled_avg_price")
    return f"status={st} id={oid} client_order_id={coid} filled_qty={fq} avg={fap}"


def _reconcile_pending():
    """
    Reconcile PENDING_ENTRY and PENDING_EXIT lots against Alpaca orders.
    Self-healing after timeouts / delayed fills.
    NOTE: Mutates DB, so call only in LIVE mode.
    """
    msgs = []

    # ---- Pending entries ----
    pe = get_pending_entries()
    if pe is not None and not pe.empty:
        for _, r in pe.iterrows():
            sym = r["symbol"]
            coid = r.get("entry_client_order_id")
            oid = r.get("entry_order_id")

            try:
                if oid:
                    o = get_order(oid)
                elif coid:
                    o = get_order_by_client_order_id(coid)
                    if o is None:
                        msgs.append(f"⚠️ PENDING_ENTRY {sym}: order not found (client_order_id={coid})")
                        continue
                else:
                    msgs.append(f"⚠️ PENDING_ENTRY {sym}: missing order ids")
                    continue

                st = (o.get("status") or "").lower()
                if st == "filled":
                    filled_qty = _safe_float(o.get("filled_qty", 0.0))
                    avg_entry = _safe_float(o.get("filled_avg_price", 0.0))
                    filled_notional = filled_qty * avg_entry

                    mark_lot_open_filled(
                        coid,
                        entry_order_id=o.get("id") or oid or "UNKNOWN",
                        qty=filled_qty,
                        avg_entry_price=avg_entry,
                        filled_notional=filled_notional,
                        filled_at=o.get("filled_at") or datetime.now(ET).isoformat(),
                    )
                    msgs.append(f"✅ Reconciled ENTRY fill: {sym} qty={filled_qty:.6f} avg=${avg_entry:.2f}")

                elif st in ("canceled", "rejected", "expired"):
                    mark_lot_failed(coid, f"reconciled_terminal:{st}")
                    msgs.append(f"🛑 ENTRY terminal {sym}: {st}")

            except Exception as e:
                msgs.append(f"❌ Reconcile PENDING_ENTRY {sym} failed: {e}")

    # ---- Pending exits ----
    px = get_pending_exits()
    if px is not None and not px.empty:
        # exits are per-symbol+exit_date cohorts; dedupe
        seen = set()
        for _, r in px.iterrows():
            sym = r["symbol"]
            exit_date = r["exit_date"]
            key = (sym, exit_date)
            if key in seen:
                continue
            seen.add(key)

            coid = r.get("exit_client_order_id")
            oid = r.get("exit_order_id")

            try:
                if oid:
                    o = get_order(oid)
                elif coid:
                    o = get_order_by_client_order_id(coid)
                    if o is None:
                        msgs.append(f"⚠️ PENDING_EXIT {sym} {exit_date}: order not found (client_order_id={coid})")
                        continue
                else:
                    msgs.append(f"⚠️ PENDING_EXIT {sym} {exit_date}: missing order ids")
                    continue

                st = (o.get("status") or "").lower()
                if st == "filled":
                    filled_qty = _safe_float(o.get("filled_qty", 0.0))
                    avg_exit = _safe_float(o.get("filled_avg_price", 0.0))
                    filled_notional_exit = filled_qty * avg_exit

                    close_lots_for_symbol_exitdate_filled(
                        sym,
                        exit_date,
                        avg_exit_price=avg_exit,
                        filled_notional_exit=filled_notional_exit,
                        filled_at=o.get("filled_at") or datetime.now(ET).isoformat(),
                        sold_qty_total=filled_qty,
                    )
                    msgs.append(f"✅ Reconciled EXIT fill: {sym} {exit_date} qty={filled_qty:.6f} avg=${avg_exit:.2f}")

                elif st in ("canceled", "rejected", "expired"):
                    reopen_pending_exit(sym, exit_date, f"reconciled_terminal:{st}")
                    msgs.append(f"🛑 EXIT terminal {sym} {exit_date}: {st} (reopened lots)")

            except Exception as e:
                msgs.append(f"❌ Reconcile PENDING_EXIT {sym} {exit_date} failed: {e}")

    return msgs


def main():
    require_env()
    init_db()

    now_et = datetime.now(ET)
    run_date = now_et.date()
    run_date_str = str(run_date)

    cal = get_trading_calendar(start=str(run_date - timedelta(days=10)), end=str(run_date + timedelta(days=60)))
    if cal.empty:
        raise RuntimeError("Trading calendar empty.")

    # --- Guard: cron may run on weekends/holidays. Exit unless forced test date.
    cal_dates = set(cal["date"].tolist())
    if (not FORCE_EXEC_DATE) and (run_date not in cal_dates):
        print(f"Not a trading day ({run_date}); exiting.")
        return

    # Determine exec_date (the trading day at open we execute)
    if FORCE_EXEC_DATE:
        exec_date_str = FORCE_EXEC_DATE
        exec_date = datetime.strptime(exec_date_str, "%Y-%m-%d").date()
    else:
        if run_date not in cal_dates:
            exec_date = cal[cal["date"] > run_date].iloc[0]["date"]
        else:
            exec_date = run_date
        exec_date_str = str(exec_date)

    # 0) Reconcile pending orders first (LIVE only)
    rec_msgs = []
    if not DRY_RUN:
        rec_msgs = _reconcile_pending()

    # Load plan (may be missing → SELL-ONLY mode)
    plan = get_plan(exec_date_str)

    sell_only_mode = plan is None
    if sell_only_mode:
        plan = {"gate_ok": False, "buy_symbols": [], "executed": False, "plan_date": exec_date_str}
        log_event("AT_OPEN", f"No plan for {exec_date_str}; SELL-ONLY mode.")

    # ✅ Key structural fix:
    # If plan already executed, we STILL run EXITS, but we SKIP BUYS.
    skip_buys = False
    if (not sell_only_mode) and plan_already_executed(exec_date_str):
        skip_buys = True
        log_event("AT_OPEN", f"Plan already executed for {exec_date_str}; running EXITS only (skip BUYS).")

    # -------------------------
    # 1) EXITS (qty-based, overlap-safe)
    # -------------------------
    exiting = lots_exiting_on(exec_date_str)
    sell_msgs = []

    if not exiting.empty:
        for sym in sorted(exiting["symbol"].unique().tolist()):
            try:
                lots_today = get_open_lots_for_symbol_exitdate(sym, exec_date_str)
                if lots_today is None or lots_today.empty:
                    sell_msgs.append(f"⏭️ {sym}: no expiring OPEN lots found for {exec_date_str}.")
                    continue

                qty_to_sell = lots_today["qty"].apply(_safe_float).sum()
                if qty_to_sell <= MIN_SELL_QTY:
                    sell_msgs.append(f"⏭️ {sym}: qty_to_sell≈0 from DB lots.")
                    continue

                pos = get_position(sym)
                if not pos:
                    sell_msgs.append(f"⚠️ No Alpaca position for {sym}. (DB wants qty={qty_to_sell:.6f})")
                    continue

                broker_qty = abs(_safe_float(pos.get("qty", 0.0)))
                if broker_qty <= MIN_SELL_QTY:
                    sell_msgs.append(f"⚠️ Alpaca qty ~0 for {sym}. (DB wants qty={qty_to_sell:.6f})")
                    continue

                # Guard: never sell more than broker has (float tolerance)
                qty_to_sell = min(qty_to_sell, broker_qty)

                exit_client_order_id = f"mrbot-{exec_date_str}-{sym}-sell"

                if DRY_RUN:
                    sell_msgs.append(
                        f"🧪 DRY_RUN would SELL {sym} qty={qty_to_sell:.6f} (broker_qty={broker_qty:.6f})"
                    )
                    continue

                resp = submit_market_order(
                    symbol=sym,
                    side="sell",
                    notional=None,
                    qty=qty_to_sell,
                    time_in_force="day",
                    client_order_id=exit_client_order_id,
                )
                exit_order_id = resp.get("id")

                # Mark ONLY expiring lots as PENDING_EXIT
                mark_lots_pending_exit(sym, exec_date_str, exit_client_order_id, exit_order_id)

                o = wait_for_order_terminal(
                    order_id=exit_order_id,
                    client_order_id=None,
                    timeout_sec=FILL_TIMEOUT_SEC,
                    poll_sec=FILL_POLL_SEC,
                )

                st = (o.get("status") or "").lower()
                if st != "filled":
                    sell_msgs.append(f"⚠️ SELL not filled for {sym}: {_order_terminal_summary(o)}")
                    continue

                filled_qty = _safe_float(o.get("filled_qty", 0.0))
                avg_exit = _safe_float(o.get("filled_avg_price", 0.0))
                filled_notional_exit = filled_qty * avg_exit

                close_lots_for_symbol_exitdate_filled(
                    sym,
                    exec_date_str,
                    avg_exit_price=avg_exit,
                    filled_notional_exit=filled_notional_exit,
                    filled_at=o.get("filled_at") or datetime.now(ET).isoformat(),
                    sold_qty_total=filled_qty,
                )

                sell_msgs.append(
                    f"✅ SELL filled: {sym} qty={filled_qty:.6f} avg=${avg_exit:.2f} notional≈${filled_notional_exit:.2f}"
                )

            except Exception as e:
                sell_msgs.append(f"❌ SELL {sym} failed: {e}")
    else:
        sell_msgs.append(f"No lots exiting on {exec_date_str}.")

    # -------------------------
    # 2) ENTRIES (fill-confirmed) — skip if plan executed
    # -------------------------
    buys_allowed = (not sell_only_mode) and (not skip_buys) and bool(plan.get("gate_ok"))
    buys = plan["buy_symbols"] if buys_allowed else []

    buy_msgs = [
        f"Gate for {exec_date_str}: {'ON ✅' if plan.get('gate_ok') else 'OFF ⛔'}"
        + (" | BUYS SKIPPED (plan already executed)" if skip_buys else "")
    ]
    buy_success = 0

    if buys:
        exit_date = add_trading_days(cal, start_date=exec_date, n=HOLD_DAYS)

        for sym in buys:
            if lot_exists_for_entry(sym, exec_date_str):
                buy_msgs.append(f"⏭️ SKIP already have lot for {sym} entry={exec_date_str}")
                continue

            entry_client_order_id = f"mrbot-{exec_date_str}-{sym}-buy"

            if DRY_RUN:
                buy_msgs.append(f"🧪 DRY_RUN would BUY {sym} notional=${NOTIONAL_PER_POSITION:.2f} exit={exit_date}")
                continue

            try:
                add_lot_pending_entry(
                    symbol=sym,
                    entry_date=exec_date_str,
                    exit_date=exit_date,
                    notional=NOTIONAL_PER_POSITION,
                    entry_client_order_id=entry_client_order_id,
                )

                resp = submit_market_order(
                    symbol=sym,
                    side="buy",
                    notional=NOTIONAL_PER_POSITION,
                    qty=None,
                    time_in_force="day",
                    client_order_id=entry_client_order_id,
                )
                entry_order_id = resp.get("id")

                o = wait_for_order_terminal(
                    order_id=entry_order_id,
                    client_order_id=None,
                    timeout_sec=FILL_TIMEOUT_SEC,
                    poll_sec=FILL_POLL_SEC,
                )

                st = (o.get("status") or "").lower()
                if st != "filled":
                    buy_msgs.append(f"⚠️ BUY not filled for {sym}: {_order_terminal_summary(o)}")
                    mark_lot_failed(entry_client_order_id, f"entry_not_filled:{st}")
                    continue

                filled_qty = _safe_float(o.get("filled_qty", 0.0))
                avg_entry = _safe_float(o.get("filled_avg_price", 0.0))
                filled_notional_entry = filled_qty * avg_entry

                mark_lot_open_filled(
                    entry_client_order_id,
                    entry_order_id=o.get("id") or entry_order_id,
                    qty=filled_qty,
                    avg_entry_price=avg_entry,
                    filled_notional=filled_notional_entry,
                    filled_at=o.get("filled_at") or datetime.now(ET).isoformat(),
                )

                buy_msgs.append(
                    f"✅ BUY filled: {sym} qty={filled_qty:.6f} avg=${avg_entry:.2f} "
                    f"notional≈${filled_notional_entry:.2f} exit={exit_date}"
                )
                buy_success += 1

            except Exception as e:
                buy_msgs.append(f"❌ BUY {sym} failed: {e}")
                try:
                    mark_lot_failed(entry_client_order_id, f"exception:{e}")
                except Exception:
                    pass
    else:
        if sell_only_mode:
            buy_msgs.append("No buys (SELL-ONLY mode: no plan).")
        elif skip_buys:
            buy_msgs.append("No buys (plan already executed).")
        else:
            buy_msgs.append("No buys (gate off or empty list).")

    # -------------------------
    # 3) Mark plan executed (only if we actually ran buys logic for this plan)
    # -------------------------
    if (not DRY_RUN) and (not sell_only_mode) and (not skip_buys):
        if (not plan.get("gate_ok")) or (buy_success > 0):
            mark_plan_executed(exec_date_str)
        else:
            log_event("AT_OPEN", f"Plan NOT marked executed (all buys failed) for {exec_date_str}")

    # -------------------------
    # 4) Notify
    # -------------------------
    msg = [
        "🚀 At-Open Execution",
        f"Run date: {run_date_str}",
        f"Exec plan date: {exec_date_str}",
        f"Mode: {'DRY_RUN' if DRY_RUN else 'LIVE-PAPER'}",
    ]
    if FORCE_EXEC_DATE:
        msg.append(f"TEST override date: {FORCE_EXEC_DATE}")
    if sell_only_mode:
        msg.append("Mode note: SELL-ONLY (no plan found)")
    if skip_buys:
        msg.append("Mode note: EXITS-ONLY (plan already executed; skipping buys)")
    msg.append("")

    if rec_msgs:
        msg += ["Reconcile:"] + rec_msgs + [""]

    msg += ["Exits:"] + (sell_msgs if sell_msgs else ["None"]) + [""]
    msg += ["Entries:"] + buy_msgs

    tg_send("\n".join(msg))
    log_event("AT_OPEN", " | ".join(msg))
    print("\n".join(msg))


if __name__ == "__main__":
    main()