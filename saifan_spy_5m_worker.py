import os
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= CONFIG =========
FMP_API_KEY = os.getenv("FMP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_SPY = "saifan_intraday_candles_spy_5m"
TABLE_VIX = "saifan_intraday_vix_5m"


# ========= HELPERS =========

def fetch_symbol_5m(symbol):
    url = f"https://financialmodelingprep.com/api/v3/historical-chart/5min/{symbol}?apikey={FMP_API_KEY}"
    try:
        res = requests.get(url, timeout=10)
        data = res.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return None
    except Exception as e:
        print(f"Fetch error for {symbol}:", e)
        return None


def is_today_utc(candle_time_str: str) -> bool:
    ct = datetime.fromisoformat(candle_time_str)
    today = datetime.now(timezone.utc).date()
    return ct.date() == today


def candle_exists(table, candle_time: str) -> bool:
    resp = supabase.table(table).select("id").eq("candle_time", candle_time).execute()
    return len(resp.data) > 0


def insert_spy(bar):
    data = {
        "candle_time": bar["date"],
        "open": float(bar["open"]),
        "high": float(bar["high"]),
        "low": float(bar["low"]),
        "close": float(bar["close"]),
        "volume": int(bar["volume"])
    }
    supabase.table(TABLE_SPY).insert(data).execute()
    print("Inserted SPY:", data)


def insert_vix(bar):
    data = {
        "candle_time": bar["date"],
        "open": float(bar["open"]),
        "high": float(bar["high"]),
        "low": float(bar["low"]),
        "close": float(bar["close"])
    }
    supabase.table(TABLE_VIX).insert(data).execute()
    print("Inserted VIX:", data)


# ========= MAIN =========

def process_symbol(symbol, table, insert_func):
    bar = fetch_symbol_5m(symbol)
    if not bar:
        print(f"No data for {symbol}.")
        return

    candle_time = bar["date"]

    if not is_today_utc(candle_time):
        print(f"Skipping {symbol}: candle not from today.")
        return

    if candle_exists(table, candle_time):
        print(f"Skipping {symbol}: duplicate candle {candle_time}.")
        return

    insert_func(bar)


def main():
    print("Running combined SPY & VIX 5m worker...")

    process_symbol("SPY", TABLE_SPY, insert_spy)
    process_symbol("VIX", TABLE_VIX, insert_vix)


if __name__ == "__main__":
    main()
