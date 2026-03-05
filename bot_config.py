import os
from dotenv import load_dotenv

load_dotenv()

# --- Alpaca ---
ALPACA_KEY = os.getenv("ALPACA_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET")
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

# Market data endpoint (v2 data)
ALPACA_DATA_BASE_URL = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")

# --- Telegram ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# --- Strategy params (your current “most realistic” spec) ---
HOLD_DAYS = 5
BOTTOM_N = 10
ZSCORE_WINDOW = 5
ZSCORE_FILTER = -1.0
RSI_PERIOD = 2
LAG_DAYS = 1  # t-1 signals
MODE = "oo"   # open-to-open execution assumption

# Regime gating
DISP_LAG_DAYS = 1
LOW_Q = 0.55
ROLLING_WINDOW = 252
MIN_NAMES_FOR_DISP = 50

# Risk controls / safety
MIN_PRICE = 2.0               # avoid penny-ish
MAX_POSITIONS = 10            # should match BOTTOM_N
NOTIONAL_PER_POSITION = 200.0 # paper size per name (edit later)
USE_NOTIONAL_ORDERS = True    # if False, uses qty based on last close

# Storage
DB_PATH = os.getenv("DB_PATH", "bot_state.sqlite")
DATA_DIR = os.getenv("DATA_DIR", "data")

def require_env():
    missing = []
    for k in ["ALPACA_KEY", "ALPACA_SECRET", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]:
        if not globals().get(k):
            missing.append(k)
    if missing:
        raise RuntimeError(f"Missing required env vars: {missing}")