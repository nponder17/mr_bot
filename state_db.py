import os
import pandas as pd

# Postgres driver
import psycopg
from psycopg.rows import dict_row

# Optional local dev fallback (SQLite)
import sqlite3
from bot_config import DB_PATH

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


# -----------------------------
# Backend selection
# -----------------------------
def _use_postgres() -> bool:
    return bool(DATABASE_URL)


# -----------------------------
# Connections
# -----------------------------
def _pg_conn():
    # Render typically provides a full postgres:// URL
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def _sqlite_conn():
    return sqlite3.connect(DB_PATH)


# -----------------------------
# Init / schema
# -----------------------------
def init_db():
    """
    Creates tables if missing.
    - In Postgres (Render): creates final schema directly (fresh start).
    - In SQLite (local): keeps your existing schema behavior.
    """
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                # lots
                cur.execute("""
                CREATE TABLE IF NOT EXISTS lots (
                    lot_id BIGSERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    entry_date TEXT NOT NULL,
                    exit_date TEXT NOT NULL,
                    notional DOUBLE PRECISION NOT NULL,
                    status TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),

                    entry_client_order_id TEXT,
                    entry_order_id TEXT,
                    entry_filled_at TIMESTAMPTZ,
                    qty DOUBLE PRECISION,
                    avg_entry_price DOUBLE PRECISION,
                    filled_notional_entry DOUBLE PRECISION,

                    exit_client_order_id TEXT,
                    exit_order_id TEXT,
                    exit_filled_at TIMESTAMPTZ,
                    avg_exit_price DOUBLE PRECISION,
                    filled_notional_exit DOUBLE PRECISION,

                    fail_reason TEXT
                );
                """)

                # planned
                cur.execute("""
                CREATE TABLE IF NOT EXISTS planned (
                    plan_date TEXT PRIMARY KEY,
                    gate_ok INTEGER NOT NULL,
                    buy_symbols TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    executed INTEGER DEFAULT 0,
                    executed_at TIMESTAMPTZ
                );
                """)

                # events
                cur.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    event_id BIGSERIAL PRIMARY KEY,
                    ts TIMESTAMPTZ DEFAULT NOW(),
                    event_type TEXT,
                    message TEXT
                );
                """)

                # equity snapshots
                cur.execute("""
                CREATE TABLE IF NOT EXISTS equity_snapshots (
                    snap_date TEXT PRIMARY KEY,
                    ts TIMESTAMPTZ DEFAULT NOW(),
                    equity DOUBLE PRECISION,
                    cash DOUBLE PRECISION,
                    buying_power DOUBLE PRECISION,
                    bot_mv DOUBLE PRECISION,
                    bot_unrealized_pl DOUBLE PRECISION,
                    note TEXT
                );
                """)

                # indices
                cur.execute("CREATE INDEX IF NOT EXISTS idx_lots_status_exit ON lots(status, exit_date);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_lots_symbol_entry ON lots(symbol, entry_date);")

            c.commit()
        return

    # ---- SQLite fallback (your current implementation) ----
    with _sqlite_conn() as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS lots (
            lot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            entry_date TEXT NOT NULL,
            exit_date TEXT NOT NULL,
            notional REAL NOT NULL,
            status TEXT NOT NULL,
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

        # migrations for planned
        cols = [r[1] for r in c.execute("PRAGMA table_info(planned);").fetchall()]
        if "executed" not in cols:
            c.execute("ALTER TABLE planned ADD COLUMN executed INTEGER DEFAULT 0;")
        if "executed_at" not in cols:
            c.execute("ALTER TABLE planned ADD COLUMN executed_at TEXT;")

        # migrations for lots
        lot_cols = [r[1] for r in c.execute("PRAGMA table_info(lots);").fetchall()]

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

        # equity_snapshots
        c.execute("""
        CREATE TABLE IF NOT EXISTS equity_snapshots (
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

        try:
            c.execute("CREATE INDEX IF NOT EXISTS idx_lots_status_exit ON lots(status, exit_date);")
            c.execute("CREATE INDEX IF NOT EXISTS idx_lots_symbol_entry ON lots(symbol, entry_date);")
        except Exception:
            pass

        c.commit()


# -----------------------------
# Helpers
# -----------------------------
def log_event(event_type: str, message: str):
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("INSERT INTO events(event_type, message) VALUES (%s, %s)", (event_type, message))
            c.commit()
        return

    with _sqlite_conn() as c:
        c.execute("INSERT INTO events(event_type, message) VALUES (?, ?)", (event_type, message))
        c.commit()


def upsert_plan(plan_date: str, gate_ok: bool, buy_symbols):
    syms = ",".join(buy_symbols) if buy_symbols else ""
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                INSERT INTO planned(plan_date, gate_ok, buy_symbols, executed, executed_at)
                VALUES (%s, %s, %s, 0, NULL)
                ON CONFLICT(plan_date) DO UPDATE SET
                    gate_ok=EXCLUDED.gate_ok,
                    buy_symbols=EXCLUDED.buy_symbols,
                    created_at=NOW();
                """, (plan_date, 1 if gate_ok else 0, syms))
            c.commit()
        return

    with _sqlite_conn() as c:
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
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                    SELECT plan_date, gate_ok, buy_symbols, COALESCE(executed,0) AS executed
                    FROM planned WHERE plan_date=%s
                """, (plan_date,))
                row = cur.fetchone()
        if not row:
            return None
        return {
            "plan_date": row["plan_date"],
            "gate_ok": bool(row["gate_ok"]),
            "buy_symbols": [s for s in (row["buy_symbols"] or "").split(",") if s],
            "executed": bool(row["executed"]),
        }

    with _sqlite_conn() as c:
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
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT COALESCE(executed,0) AS executed FROM planned WHERE plan_date=%s", (plan_date,))
                row = cur.fetchone()
        return bool(row and int(row["executed"]) == 1)

    with _sqlite_conn() as c:
        cur = c.execute("SELECT COALESCE(executed,0) FROM planned WHERE plan_date=?", (plan_date,))
        row = cur.fetchone()
    return bool(row and int(row[0]) == 1)


def mark_plan_executed(plan_date: str):
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                    UPDATE planned
                    SET executed=1, executed_at=NOW()
                    WHERE plan_date=%s
                """, (plan_date,))
            c.commit()
        return

    with _sqlite_conn() as c:
        c.execute("""
            UPDATE planned
            SET executed=1, executed_at=datetime('now')
            WHERE plan_date=?
        """, (plan_date,))
        c.commit()


# -----------------------------
# Lots lifecycle (same semantics)
# -----------------------------
def lot_exists_for_entry(symbol: str, entry_date: str) -> bool:
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM lots
                    WHERE symbol=%s AND entry_date=%s AND status IN ('PENDING_ENTRY','OPEN','PENDING_EXIT')
                    LIMIT 1
                """, (symbol, entry_date))
                return cur.fetchone() is not None

    with _sqlite_conn() as c:
        cur = c.execute("""
            SELECT 1 FROM lots
            WHERE symbol=? AND entry_date=? AND status IN ('PENDING_ENTRY','OPEN','PENDING_EXIT')
            LIMIT 1
        """, (symbol, entry_date))
        return cur.fetchone() is not None


def add_lot_pending_entry(symbol: str, entry_date: str, exit_date: str, notional: float, entry_client_order_id: str):
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                INSERT INTO lots(symbol, entry_date, exit_date, notional, status, entry_client_order_id)
                VALUES (%s, %s, %s, %s, 'PENDING_ENTRY', %s)
                """, (symbol, entry_date, exit_date, float(notional), entry_client_order_id))
            c.commit()
        return

    with _sqlite_conn() as c:
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
    if _use_postgres():
        allowed = ("PENDING_ENTRY", "FAILED") if allow_failed else ("PENDING_ENTRY",)
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute(f"""
                UPDATE lots
                SET status='OPEN',
                    entry_order_id=%s,
                    qty=%s,
                    avg_entry_price=%s,
                    filled_notional_entry=%s,
                    entry_filled_at=%s,
                    fail_reason=NULL
                WHERE entry_client_order_id=%s
                  AND status = ANY(%s)
                """, (
                    entry_order_id,
                    float(qty),
                    float(avg_entry_price),
                    float(filled_notional),
                    filled_at,
                    entry_client_order_id,
                    list(allowed),
                ))
            c.commit()
        return

    allowed = ("PENDING_ENTRY", "FAILED") if allow_failed else ("PENDING_ENTRY",)
    qmarks = ",".join(["?"] * len(allowed))
    with _sqlite_conn() as c:
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
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                UPDATE lots
                SET status='FAILED', fail_reason=%s
                WHERE entry_client_order_id=%s AND status='PENDING_ENTRY'
                """, (reason, entry_client_order_id))
            c.commit()
        return

    with _sqlite_conn() as c:
        c.execute("""
        UPDATE lots
        SET status='FAILED', fail_reason=?
        WHERE entry_client_order_id=? AND status='PENDING_ENTRY'
        """, (reason, entry_client_order_id))
        c.commit()


def lots_exiting_on(date_str: str) -> pd.DataFrame:
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute(
                    "SELECT * FROM lots WHERE status IN ('OPEN','PENDING_EXIT') AND exit_date=%s",
                    (date_str,)
                )
                rows = cur.fetchall()
        return pd.DataFrame(rows)

    with _sqlite_conn() as c:
        return pd.read_sql_query(
            "SELECT * FROM lots WHERE status IN ('OPEN','PENDING_EXIT') AND exit_date=?",
            c, params=(date_str,)
        )


def get_open_lots_for_symbol_exitdate(symbol: str, exit_date: str) -> pd.DataFrame:
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                SELECT * FROM lots
                WHERE symbol=%s AND exit_date=%s AND status IN ('OPEN','PENDING_EXIT')
                ORDER BY lot_id ASC
                """, (symbol, exit_date))
                rows = cur.fetchall()
        return pd.DataFrame(rows)

    with _sqlite_conn() as c:
        return pd.read_sql_query(
            "SELECT * FROM lots WHERE symbol=? AND exit_date=? AND status IN ('OPEN','PENDING_EXIT') ORDER BY lot_id ASC",
            c, params=(symbol, exit_date)
        )


def mark_lots_pending_exit(symbol: str, exit_date: str, exit_client_order_id: str, exit_order_id: str):
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                    UPDATE lots
                    SET status='PENDING_EXIT',
                        exit_client_order_id=%s,
                        exit_order_id=%s
                    WHERE status='OPEN' AND symbol=%s AND exit_date=%s
                """, (exit_client_order_id, exit_order_id, symbol, exit_date))
            c.commit()
        return

    with _sqlite_conn() as c:
        c.execute("""
            UPDATE lots
            SET status='PENDING_EXIT',
                exit_client_order_id=?,
                exit_order_id=?
            WHERE status='OPEN' AND symbol=? AND exit_date=?
        """, (exit_client_order_id, exit_order_id, symbol, exit_date))
        c.commit()


def reopen_pending_exit(symbol: str, exit_date: str, reason: str):
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                    UPDATE lots
                    SET status='OPEN',
                        fail_reason=%s,
                        exit_client_order_id=NULL,
                        exit_order_id=NULL
                    WHERE status='PENDING_EXIT' AND symbol=%s AND exit_date=%s
                """, (reason, symbol, exit_date))
            c.commit()
        return

    with _sqlite_conn() as c:
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
    lots = get_open_lots_for_symbol_exitdate(symbol, exit_date)
    if lots.empty:
        return

    qty_sum = float(lots["qty"].fillna(0.0).sum())
    use_qty = qty_sum > 0 and sold_qty_total is not None and sold_qty_total > 0

    notional_sum = float(lots["notional"].fillna(0.0).sum())
    if notional_sum <= 0:
        notional_sum = 1.0

    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                for _, r in lots.iterrows():
                    lot_id = int(r["lot_id"])

                    if use_qty:
                        w = float(r["qty"]) / qty_sum if qty_sum > 0 else 0.0
                    else:
                        w = float(r["notional"]) / notional_sum

                    alloc_qty = float(sold_qty_total) * w if (sold_qty_total is not None) else None
                    alloc_notional = float(filled_notional_exit) * w

                    cur.execute("""
                        UPDATE lots
                        SET status='CLOSED',
                            avg_exit_price=%s,
                            filled_notional_exit=%s,
                            exit_filled_at=%s,
                            qty=COALESCE(qty, %s)
                        WHERE lot_id=%s AND status IN ('OPEN','PENDING_EXIT')
                    """, (float(avg_exit_price), alloc_notional, filled_at, alloc_qty, lot_id))
            c.commit()
        return

    with _sqlite_conn() as c:
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

    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT * FROM lots WHERE status = ANY(%s)", (statuses,))
                rows = cur.fetchall()
        return pd.DataFrame(rows)

    qmarks = ",".join(["?"] * len(statuses))
    with _sqlite_conn() as c:
        return pd.read_sql_query(
            f"SELECT * FROM lots WHERE status IN ({qmarks})",
            c, params=tuple(statuses)
        )


def get_pending_entries() -> pd.DataFrame:
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT * FROM lots WHERE status='PENDING_ENTRY' ORDER BY lot_id ASC")
                rows = cur.fetchall()
        return pd.DataFrame(rows)

    with _sqlite_conn() as c:
        return pd.read_sql_query(
            "SELECT * FROM lots WHERE status='PENDING_ENTRY' ORDER BY lot_id ASC",
            c
        )


def get_pending_exits() -> pd.DataFrame:
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT * FROM lots WHERE status='PENDING_EXIT' ORDER BY lot_id ASC")
                rows = cur.fetchall()
        return pd.DataFrame(rows)

    with _sqlite_conn() as c:
        return pd.read_sql_query(
            "SELECT * FROM lots WHERE status='PENDING_EXIT' ORDER BY lot_id ASC",
            c
        )


def get_recent_failed_entries(days: int = 7) -> pd.DataFrame:
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                SELECT *
                FROM lots
                WHERE status='FAILED'
                  AND entry_client_order_id IS NOT NULL
                  AND created_at >= (NOW() - (%s || ' days')::interval)
                ORDER BY lot_id ASC
                """, (int(days),))
                rows = cur.fetchall()
        return pd.DataFrame(rows)

    with _sqlite_conn() as c:
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


def upsert_equity_snapshot(
    snap_date: str,
    equity: float,
    cash: float,
    buying_power: float,
    bot_mv: float,
    bot_unrealized_pl: float,
    note: str = ""
):
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("""
                INSERT INTO equity_snapshots(
                    snap_date, equity, cash, buying_power,
                    bot_mv, bot_unrealized_pl, note
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(snap_date) DO UPDATE SET
                    ts=NOW(),
                    equity=EXCLUDED.equity,
                    cash=EXCLUDED.cash,
                    buying_power=EXCLUDED.buying_power,
                    bot_mv=EXCLUDED.bot_mv,
                    bot_unrealized_pl=EXCLUDED.bot_unrealized_pl,
                    note=EXCLUDED.note;
                """, (snap_date, equity, cash, buying_power, bot_mv, bot_unrealized_pl, note))
            c.commit()
        return

    with _sqlite_conn() as c:
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
        """, (snap_date, equity, cash, buying_power, bot_mv, bot_unrealized_pl, note))
        c.commit()


def get_equity_snapshots() -> pd.DataFrame:
    if _use_postgres():
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT * FROM equity_snapshots ORDER BY snap_date ASC")
                rows = cur.fetchall()
        return pd.DataFrame(rows)

    with _sqlite_conn() as c:
        return pd.read_sql_query("SELECT * FROM equity_snapshots ORDER BY snap_date ASC", c)