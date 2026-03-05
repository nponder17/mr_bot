import sqlite3
import pandas as pd
from bot_config import DB_PATH


def _conn():
    return sqlite3.connect(DB_PATH)


def _colnames(conn, table: str):
    cur = conn.execute(f"PRAGMA table_info({table});")
    return [r[1] for r in cur.fetchall()]


def _table_exists(conn, table: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
        (table,)
    )
    return cur.fetchone() is not None


def init_db():
    with _conn() as c:
        # --- core tables ---
        c.execute("""
        CREATE TABLE IF NOT EXISTS lots (
            lot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            entry_date TEXT NOT NULL,
            exit_date TEXT NOT NULL,
            notional REAL NOT NULL,
            status TEXT NOT NULL, -- PENDING_ENTRY / OPEN / PENDING_EXIT / CLOSED / FAILED
            created_at TEXT DEFAULT (datetime('now'))
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS planned (
            plan_date TEXT PRIMARY KEY,
            gate_ok INTEGER NOT NULL,
            buy_symbols TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT DEFAULT (datetime('now')),
            event_type TEXT,
            message TEXT
        );
        """)

        # --- migrations for planned ---
        cols = _colnames(c, "planned")
        if "executed" not in cols:
            c.execute("ALTER TABLE planned ADD COLUMN executed INTEGER DEFAULT 0;")
        if "executed_at" not in cols:
            c.execute("ALTER TABLE planned ADD COLUMN executed_at TEXT;")

        # --- migrations for lots (fill-truth fields) ---
        lot_cols = _colnames(c, "lots")

        def add_col(name: str, ddl: str):
            nonlocal lot_cols
            if name not in lot_cols:
                c.execute(f"ALTER TABLE lots ADD COLUMN {ddl};")
                lot_cols.append(name)

        add_col("entry_client_order_id", "entry_client_order_id TEXT")
        add_col("entry_order_id", "entry_order_id TEXT")
        add_col("entry_filled_at", "entry_filled_at TEXT")
        add_col("qty", "qty REAL")
        add_col("avg_entry_price", "avg_entry_price REAL")
        add_col("filled_notional_entry", "filled_notional_entry REAL")

        add_col("exit_client_order_id", "exit_client_order_id TEXT")
        add_col("exit_order_id", "exit_order_id TEXT")
        add_col("exit_filled_at", "exit_filled_at TEXT")
        add_col("avg_exit_price", "avg_exit_price REAL")
        add_col("filled_notional_exit", "filled_notional_exit REAL")

        add_col("fail_reason", "fail_reason TEXT")

        # Normalize legacy statuses if present
        c.execute("UPDATE lots SET status='OPEN' WHERE status='open';")
        c.execute("UPDATE lots SET status='CLOSED' WHERE status='closed';")

        # --- equity_snapshots ---
        if not _table_exists(c, "equity_snapshots"):
            c.execute("""
            CREATE TABLE equity_snapshots (
                snap_date TEXT PRIMARY KEY,
                ts TEXT DEFAULT (datetime('now')),
                equity REAL,
                cash REAL,
                buying_power REAL,
                bot_mv REAL,
                bot_unrealized_pl REAL,
                note TEXT
            );
            """)

        # Helpful indices
        try:
            c.execute("CREATE INDEX IF NOT EXISTS idx_lots_status_exit ON lots(status, exit_date);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_lots_symbol_entry ON lots(symbol, entry_date);")
        except Exception:
            pass

        c.commit()


def log_event(event_type: str, message: str):
    with _conn() as c:
        c.execute("INSERT INTO events(event_type, message) VALUES (?, ?)", (event_type, message))
        c.commit()


def upsert_plan(plan_date: str, gate_ok: bool, buy_symbols):
    syms = ",".join(buy_symbols) if buy_symbols else ""
    with _conn() as c:
        c.execute("""
        INSERT INTO planned(plan_date, gate_ok, buy_symbols, executed, executed_at)
        VALUES (?, ?, ?, 0, NULL)
        ON CONFLICT(plan_date) DO UPDATE SET
            gate_ok=excluded.gate_ok,
            buy_symbols=excluded.buy_symbols,
            created_at=datetime('now');
        """, (plan_date, 1 if gate_ok else 0, syms))
        c.commit()


def get_plan(plan_date: str):
    with _conn() as c:
        cur = c.execute("""
            SELECT plan_date, gate_ok, buy_symbols, COALESCE(executed,0)
            FROM planned WHERE plan_date=?
        """, (plan_date,))
        row = cur.fetchone()
    if not row:
        return None
    return {
        "plan_date": row[0],
        "gate_ok": bool(row[1]),
        "buy_symbols": [s for s in (row[2] or "").split(",") if s],
        "executed": bool(row[3]),
    }


def plan_already_executed(plan_date: str) -> bool:
    with _conn() as c:
        cur = c.execute("SELECT COALESCE(executed,0) FROM planned WHERE plan_date=?", (plan_date,))
        row = cur.fetchone()
    return bool(row and int(row[0]) == 1)


def mark_plan_executed(plan_date: str):
    with _conn() as c:
        c.execute("""
            UPDATE planned
            SET executed=1, executed_at=datetime('now')
            WHERE plan_date=?
        """, (plan_date,))
        c.commit()


# ---------- Lots lifecycle ----------

def lot_exists_for_entry(symbol: str, entry_date: str) -> bool:
    with _conn() as c:
        cur = c.execute("""
            SELECT 1 FROM lots
            WHERE symbol=? AND entry_date=? AND status IN ('PENDING_ENTRY','OPEN','PENDING_EXIT')
            LIMIT 1
        """, (symbol, entry_date))
        return cur.fetchone() is not None


def add_lot_pending_entry(symbol: str, entry_date: str, exit_date: str, notional: float, entry_client_order_id: str):
    with _conn() as c:
        c.execute("""
        INSERT INTO lots(symbol, entry_date, exit_date, notional, status, entry_client_order_id)
        VALUES (?, ?, ?, ?, 'PENDING_ENTRY', ?)
        """, (symbol, entry_date, exit_date, float(notional), entry_client_order_id))
        c.commit()


def mark_lot_open_filled(
    entry_client_order_id: str,
    *,
    entry_order_id: str,
    qty: float,
    avg_entry_price: float,
    filled_notional: float,
    filled_at: str,
    allow_failed: bool = False,
):
    """
    Normal path: PENDING_ENTRY -> OPEN
    If allow_failed=True, also allows FAILED -> OPEN (for late fills / misclassified timeouts).
    """
    allowed = ("PENDING_ENTRY", "FAILED") if allow_failed else ("PENDING_ENTRY",)
    qmarks = ",".join(["?"] * len(allowed))

    with _conn() as c:
        c.execute(f"""
        UPDATE lots
        SET status='OPEN',
            entry_order_id=?,
            qty=?,
            avg_entry_price=?,
            filled_notional_entry=?,
            entry_filled_at=?,
            fail_reason=NULL
        WHERE entry_client_order_id=?
          AND status IN ({qmarks})
        """, (
            entry_order_id,
            float(qty),
            float(avg_entry_price),
            float(filled_notional),
            filled_at,
            entry_client_order_id,
            *allowed
        ))
        c.commit()


def mark_lot_failed(entry_client_order_id: str, reason: str):
    with _conn() as c:
        c.execute("""
        UPDATE lots
        SET status='FAILED', fail_reason=?
        WHERE entry_client_order_id=? AND status='PENDING_ENTRY'
        """, (reason, entry_client_order_id))
        c.commit()


def lots_exiting_on(date_str: str) -> pd.DataFrame:
    with _conn() as c:
        df = pd.read_sql_query(
            "SELECT * FROM lots WHERE status IN ('OPEN','PENDING_EXIT') AND exit_date=?",
            c, params=(date_str,)
        )
    return df


def get_open_lots_for_symbol_exitdate(symbol: str, exit_date: str) -> pd.DataFrame:
    with _conn() as c:
        df = pd.read_sql_query(
            "SELECT * FROM lots WHERE symbol=? AND exit_date=? AND status IN ('OPEN','PENDING_EXIT') ORDER BY lot_id ASC",
            c, params=(symbol, exit_date)
        )
    return df


def mark_lots_pending_exit(symbol: str, exit_date: str, exit_client_order_id: str, exit_order_id: str):
    with _conn() as c:
        c.execute("""
            UPDATE lots
            SET status='PENDING_EXIT',
                exit_client_order_id=?,
                exit_order_id=?
            WHERE status='OPEN' AND symbol=? AND exit_date=?
        """, (exit_client_order_id, exit_order_id, symbol, exit_date))
        c.commit()


def reopen_pending_exit(symbol: str, exit_date: str, reason: str):
    """
    If an exit order is canceled/rejected/expired, revert lots back to OPEN so next run can attempt again.
    """
    with _conn() as c:
        c.execute("""
            UPDATE lots
            SET status='OPEN',
                fail_reason=?,
                exit_client_order_id=NULL,
                exit_order_id=NULL
            WHERE status='PENDING_EXIT' AND symbol=? AND exit_date=?
        """, (reason, symbol, exit_date))
        c.commit()


def close_lots_for_symbol_exitdate_filled(
    symbol: str,
    exit_date: str,
    *,
    avg_exit_price: float,
    filled_notional_exit: float,
    filled_at: str,
    sold_qty_total: float | None = None
):
    """
    Mark expiring lots CLOSED only after sell is filled.
    Allocation is by LOT QTY (not notional) when sold_qty_total is known.
    """
    lots = get_open_lots_for_symbol_exitdate(symbol, exit_date)
    if lots.empty:
        return

    qty_sum = float(lots["qty"].fillna(0.0).sum())
    use_qty = qty_sum > 0 and sold_qty_total is not None and sold_qty_total > 0

    notional_sum = float(lots["notional"].fillna(0.0).sum())
    if notional_sum <= 0:
        notional_sum = 1.0

    with _conn() as c:
        for _, r in lots.iterrows():
            lot_id = int(r["lot_id"])

            if use_qty:
                w = float(r["qty"]) / qty_sum if qty_sum > 0 else 0.0
            else:
                w = float(r["notional"]) / notional_sum

            alloc_qty = float(sold_qty_total) * w if (sold_qty_total is not None) else None
            alloc_notional = float(filled_notional_exit) * w

            c.execute("""
                UPDATE lots
                SET status='CLOSED',
                    avg_exit_price=?,
                    filled_notional_exit=?,
                    exit_filled_at=?,
                    qty=COALESCE(qty, ?)
                WHERE lot_id=? AND status IN ('OPEN','PENDING_EXIT')
            """, (float(avg_exit_price), alloc_notional, filled_at, alloc_qty, lot_id))
        c.commit()


def open_lots(include_pending_entry: bool = False) -> pd.DataFrame:
    statuses = ["OPEN", "PENDING_EXIT"]
    if include_pending_entry:
        statuses = ["PENDING_ENTRY"] + statuses
    qmarks = ",".join(["?"] * len(statuses))
    with _conn() as c:
        df = pd.read_sql_query(
            f"SELECT * FROM lots WHERE status IN ({qmarks})",
            c, params=tuple(statuses)
        )
    return df


# ---------- Pending helpers ----------

def get_pending_entries() -> pd.DataFrame:
    with _conn() as c:
        return pd.read_sql_query(
            "SELECT * FROM lots WHERE status='PENDING_ENTRY' ORDER BY lot_id ASC",
            c
        )


def get_pending_exits() -> pd.DataFrame:
    with _conn() as c:
        return pd.read_sql_query(
            "SELECT * FROM lots WHERE status='PENDING_EXIT' ORDER BY lot_id ASC",
            c
        )


def get_recent_failed_entries(days: int = 7) -> pd.DataFrame:
    """
    Pull FAILED lots that still have an entry_client_order_id, so we can detect late fills.
    """
    with _conn() as c:
        return pd.read_sql_query(
            """
            SELECT *
            FROM lots
            WHERE status='FAILED'
              AND entry_client_order_id IS NOT NULL
              AND created_at >= datetime('now', ?)
            ORDER BY lot_id ASC
            """,
            c,
            params=(f"-{int(days)} days",)
        )


# ---------- snapshots helpers ----------

def upsert_equity_snapshot(
    snap_date: str,
    equity: float,
    cash: float,
    buying_power: float,
    bot_mv: float,
    bot_unrealized_pl: float,
    note: str = ""
):
    with _conn() as c:
        c.execute("""
        INSERT INTO equity_snapshots(
            snap_date, equity, cash, buying_power,
            bot_mv, bot_unrealized_pl, note
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(snap_date) DO UPDATE SET
            ts=datetime('now'),
            equity=excluded.equity,
            cash=excluded.cash,
            buying_power=excluded.buying_power,
            bot_mv=excluded.bot_mv,
            bot_unrealized_pl=excluded.bot_unrealized_pl,
            note=excluded.note;
        """, (
            snap_date, equity, cash, buying_power, bot_mv, bot_unrealized_pl, note
        ))
        c.commit()


def get_equity_snapshots() -> pd.DataFrame:
    with _conn() as c:
        return pd.read_sql_query("SELECT * FROM equity_snapshots ORDER BY snap_date ASC", c)