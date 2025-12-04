import os
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# ========= CONFIG =========
FMP_API_KEY = os.getenv("ZT73H6uSdo3b3kAmcjPxH5EGX0odj7MJ")
SUPABASE_URL = os.getenv("https://analkikdsytxkavvmulf.supabase.co")
SUPABASE_KEY = os.getenv("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFuYWxraWtkc3l0eGthdnZtdWxmIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NDgyNTI2MCwiZXhwIjoyMDgwNDAxMjYwfQ.mochVG2SkWq8ytaMkYTwZsSyyTEkbbvpupIesDQ5j3E")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "saifan_intraday_candles_spy_5m"


# ========= HELPERS =========

def fetch_spy_5m():
    """
    Fetch the latest 5-minute SPY candle from FMP.
    """
    url = f"https://financialmodelingprep.com/api/v3/historical-chart/5min/SPY?apikey={FMP_API_KEY}"
    try:
        res = requests.get(url, timeout=10)
        data = res.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0]  # Most recent candle
        return None
    except Exception as e:
        print("Fetch error:", e)
        return None


def is_today_utc(candle_time_str: str) -> bool:
    """
    Check if the candle timestamp belongs to the current UTC day.
    """
    ct = datetime.fromisoformat(candle_time_str)
    today = datetime.now(timezone.utc).date()
    return ct.date() == today


def candle_exists(candle_time: str) -> bool:
    """
    Check whether a candle with the same timestamp already exists.
    Prevents duplicates.
    """
    resp = supabase.table(TABLE_NAME).select("id").eq("candle_time", candle_time).execute()
    return len(resp.data) > 0


def insert_candle(bar):
    """
    Insert a new candle into the table.
    """
    data = {
        "candle_time": bar["date"],
        "open": float(bar["open"]),
        "high": float(bar["high"]),
        "low": float(bar["low"]),
        "close": float(bar["close"]),
        "volume": int(bar["volume"])
    }

    supabase.table(TABLE_NAME).insert(data).execute()
    print("Inserted:", data)


# ========= MAIN =========

def main():
    print("Running SPY 5m worker...")

    bar = fetch_spy_5m()
    if not bar:
        print("No candle fetched.")
        return

    candle_time = bar["date"]

    # Skip if candle is not from today (important for accurate VWAP)
    if not is_today_utc(candle_time):
        print(f"Skipping: candle {candle_time} is not from today.")
        return

    # Skip if candle already exists
    if candle_exists(candle_time):
        print(f"Skipping: candle {candle_time} already exists.")
        return

    # Insert candle
    insert_candle(bar)


if __name__ == "__main__":
    main()
