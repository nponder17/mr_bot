from datetime import datetime
import pytz

from bot_config import require_env
from alpaca_utils import get_order, get_order_by_client_order_id
from state_db import (
    init_db,
    get_pending_entries,
    get_pending_exits,
    get_recent_failed_entries,
    mark_lot_open_filled,
    mark_lot_failed,
    close_lots_for_symbol_exitdate_filled,
    reopen_pending_exit,
)

ET = pytz.timezone("America/New_York")


def _safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return float(default)


def main():
    require_env()
    init_db()

    now_iso = datetime.now(ET).isoformat()
    print(f"🔁 Reconcile Pending ({now_iso})\n")

    msgs = []

    # -----------------------------
    # 1) PENDING_ENTRY reconciliation
    # -----------------------------
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
                        allow_failed=False,
                    )
                    msgs.append(f"✅ ENTRY filled reconciled: {sym} qty={filled_qty:.6f} avg=${avg_entry:.2f}")

                elif st in ("canceled", "rejected", "expired"):
                    # terminal = safe to fail
                    if coid:
                        mark_lot_failed(coid, f"reconciled_terminal:{st}")
                    msgs.append(f"🛑 ENTRY terminal: {sym} status={st}")

                else:
                    msgs.append(f"⏳ ENTRY still pending: {sym} status={st}")

            except Exception as e:
                msgs.append(f"❌ Reconcile PENDING_ENTRY {sym} failed: {e}")
    else:
        msgs.append("No PENDING_ENTRY lots.")

    # -----------------------------
    # 2) PENDING_EXIT reconciliation
    # -----------------------------
    px = get_pending_exits()
    if px is not None and not px.empty:
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
                    msgs.append(f"✅ EXIT filled reconciled: {sym} {exit_date} qty={filled_qty:.6f} avg=${avg_exit:.2f}")

                elif st in ("canceled", "rejected", "expired"):
                    reopen_pending_exit(sym, exit_date, f"reconciled_terminal:{st}")
                    msgs.append(f"🛑 EXIT terminal: {sym} {exit_date} status={st} (reopened lots)")

                else:
                    msgs.append(f"⏳ EXIT still pending: {sym} {exit_date} status={st}")

            except Exception as e:
                msgs.append(f"❌ Reconcile PENDING_EXIT {sym} {exit_date} failed: {e}")
    else:
        msgs.append("No PENDING_EXIT lots.")

    # -----------------------------------------------------------------
    # 3) Reconcile “FAILED but actually filled” (your BABA exact problem)
    # -----------------------------------------------------------------
    failed = get_recent_failed_entries(days=10)
    if failed is not None and not failed.empty:
        for _, r in failed.iterrows():
            sym = r["symbol"]
            coid = r.get("entry_client_order_id")
            oid = r.get("entry_order_id")

            # only worth trying if we have at least a client_order_id
            if not coid and not oid:
                continue

            try:
                if oid:
                    o = get_order(oid)
                else:
                    o = get_order_by_client_order_id(coid)

                if o is None:
                    continue

                st = (o.get("status") or "").lower()
                if st != "filled":
                    continue

                filled_qty = _safe_float(o.get("filled_qty", 0.0))
                avg_entry = _safe_float(o.get("filled_avg_price", 0.0))
                filled_notional = filled_qty * avg_entry

                # flip FAILED -> OPEN
                mark_lot_open_filled(
                    coid,
                    entry_order_id=o.get("id") or oid or "UNKNOWN",
                    qty=filled_qty,
                    avg_entry_price=avg_entry,
                    filled_notional=filled_notional,
                    filled_at=o.get("filled_at") or datetime.now(ET).isoformat(),
                    allow_failed=True,
                )
                msgs.append(f"🧯 FIXED FAILED->OPEN: {sym} qty={filled_qty:.6f} avg=${avg_entry:.2f} (coid={coid})")

            except Exception:
                # keep it quiet; this is best-effort
                pass
    else:
        msgs.append("No recent FAILED entries to check.")

    print("\n".join(msgs))


if __name__ == "__main__":
    main()