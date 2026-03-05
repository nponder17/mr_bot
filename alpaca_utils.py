import os
import time
import requests
import pandas as pd
from bot_config import ALPACA_KEY, ALPACA_SECRET, ALPACA_BASE_URL, ALPACA_DATA_BASE_URL


def _headers() -> dict:
    return {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }


def _request_with_retries(method: str, url: str, *, params=None, json=None, timeout=30, max_retries=3):
    backoff = 1.0
    last_exc = None

    for attempt in range(max_retries):
        try:
            r = requests.request(
                method,
                url,
                headers=_headers(),
                params=params,
                json=json,
                timeout=timeout,
            )

            # Retry on rate limit / server errors
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt < max_retries - 1:
                    time.sleep(backoff)
                    backoff *= 2
                    continue

            return r

        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            if attempt < max_retries - 1:
                time.sleep(backoff)
                backoff *= 2
                continue
            raise

    if last_exc:
        raise last_exc
    raise RuntimeError("Request failed unexpectedly")


def alpaca_get(path: str, params=None, is_data: bool = False):
    base = ALPACA_DATA_BASE_URL if is_data else ALPACA_BASE_URL
    url = base.rstrip("/") + path
    r = _request_with_retries("GET", url, params=params)

    if not r.ok:
        raise requests.HTTPError(
            f"Alpaca GET {url} failed: {r.status_code} {r.text[:300]}",
            response=r,
        )
    return r.json()


def alpaca_post(path: str, payload: dict):
    url = ALPACA_BASE_URL.rstrip("/") + path
    r = _request_with_retries("POST", url, json=payload)

    if not r.ok:
        raise requests.HTTPError(
            f"Alpaca POST {url} failed: {r.status_code} {r.text[:300]}",
            response=r,
        )
    return r.json()


def alpaca_delete(path: str, params=None):
    url = ALPACA_BASE_URL.rstrip("/") + path
    r = _request_with_retries("DELETE", url, params=params)
    return r


# ---------- Calendar ----------

def get_trading_calendar(start: str, end: str) -> pd.DataFrame:
    j = alpaca_get("/v2/calendar", params={"start": start, "end": end})
    df = pd.DataFrame(j)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def get_next_trading_day(cal: pd.DataFrame, today_date) -> str:
    future = cal[cal["date"] > today_date]
    if future.empty:
        raise RuntimeError("No next trading day found in calendar range.")
    return str(future.iloc[0]["date"])


def add_trading_days(cal: pd.DataFrame, start_date, n: int) -> str:
    dates = cal["date"].tolist()
    if start_date not in dates:
        raise RuntimeError(f"start_date {start_date} not found in calendar list.")
    idx = dates.index(start_date)
    if idx + n >= len(dates):
        raise RuntimeError("Calendar range too small for add_trading_days.")
    return str(dates[idx + n])


# ---------- Market data ----------

def _parse_bar_time(tval):
    if isinstance(tval, (int, float)):
        return pd.to_datetime(int(tval), unit="ns", utc=True)
    return pd.to_datetime(tval, utc=True)


def get_daily_bars(symbols, start: str, end: str) -> pd.DataFrame:
    all_rows = []
    chunk_size = 100
    limit = 10000

    data_feed = os.getenv("ALPACA_DATA_FEED", "iex")

    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]

        page_token = None
        while True:
            params = {
                "symbols": ",".join(chunk),
                "timeframe": "1Day",
                "start": start,
                "end": end,
                "adjustment": "all",
                "feed": data_feed,
                "limit": limit,
            }
            if page_token:
                params["page_token"] = page_token

            j = alpaca_get("/v2/stocks/bars", params=params, is_data=True)

            bars = j.get("bars", {})
            for sym, rows in bars.items():
                for r in rows:
                    all_rows.append({
                        "symbol": sym,
                        "t": r["t"],
                        "open": r["o"],
                        "high": r["h"],
                        "low": r["l"],
                        "close": r["c"],
                        "volume": r.get("v", None),
                    })

            page_token = j.get("next_page_token", None)
            if not page_token:
                break

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df

    df["t"] = df["t"].apply(_parse_bar_time)
    df["date"] = df["t"].dt.tz_convert(None).dt.date

    df = df.drop(columns=["t"])
    df = df.sort_values(["date", "symbol"]).reset_index(drop=True)
    return df


# ---------- Account / positions ----------

def get_account() -> dict:
    return alpaca_get("/v2/account")


def list_open_positions() -> pd.DataFrame:
    j = alpaca_get("/v2/positions")
    return pd.DataFrame(j)


def get_position(symbol: str):
    try:
        return alpaca_get(f"/v2/positions/{symbol}")
    except requests.HTTPError as e:
        resp = getattr(e, "response", None)
        if resp is not None and resp.status_code == 404:
            return None
        raise


def close_position(symbol: str) -> dict:
    r = alpaca_delete(f"/v2/positions/{symbol}")
    if r.status_code == 404:
        return {"status": "no_position"}
    if not r.ok:
        raise requests.HTTPError(
            f"Alpaca DELETE /v2/positions/{symbol} failed: {r.status_code} {r.text[:300]}",
            response=r,
        )
    return r.json() if r.text else {"status": "closed"}


# ---------- Orders ----------

def list_orders(status="open", limit=500) -> pd.DataFrame:
    j = alpaca_get("/v2/orders", params={"status": status, "limit": limit, "nested": "true"})
    return pd.DataFrame(j)


def get_order(order_id: str) -> dict:
    return alpaca_get(f"/v2/orders/{order_id}")


def get_order_by_client_order_id(client_order_id: str):
    """
    FIX: Alpaca expects this endpoint WITHOUT a path parameter.
    It must be called with a query param:
      GET /v2/orders:by_client_order_id?client_order_id=...
    """
    try:
        return alpaca_get("/v2/orders:by_client_order_id", params={"client_order_id": client_order_id})
    except requests.HTTPError as e:
        resp = getattr(e, "response", None)
        if resp is not None and resp.status_code == 404:
            return None
        raise


def submit_market_order(
    symbol: str,
    side: str,
    notional: float = None,
    qty: float = None,
    time_in_force="day",
    client_order_id: str = None,
):
    payload = {
        "symbol": symbol,
        "side": side,
        "type": "market",
        "time_in_force": time_in_force,
    }
    if client_order_id:
        payload["client_order_id"] = client_order_id

    if notional is not None:
        payload["notional"] = str(float(notional))
    else:
        if qty is None:
            raise ValueError("qty must be provided if notional is None")
        payload["qty"] = str(float(qty))

    return alpaca_post("/v2/orders", payload)


def wait_for_order_terminal(
    *,
    order_id: str | None = None,
    client_order_id: str | None = None,
    timeout_sec: int = 75,
    poll_sec: float = 1.5,
) -> dict:
    if not order_id and not client_order_id:
        raise ValueError("Must pass order_id or client_order_id")

    t0 = time.time()
    last = None

    while True:
        if order_id:
            last = get_order(order_id)
        else:
            last = get_order_by_client_order_id(client_order_id)
            if last is None:
                if time.time() - t0 < 10:
                    time.sleep(poll_sec)
                    continue
                raise RuntimeError(f"Order not found for client_order_id={client_order_id}")

        status = (last.get("status") or "").lower()
        if status in ("filled", "canceled", "rejected", "expired"):
            return last

        if time.time() - t0 > timeout_sec:
            return last

        time.sleep(poll_sec)