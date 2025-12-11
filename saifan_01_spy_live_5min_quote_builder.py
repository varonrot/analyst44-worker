import os
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from supabase import create_client, Client

# =============================
# CONFIG
# =============================

FMP_API_KEY = os.getenv("FMP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not FMP_API_KEY or not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("Missing environment variables")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

NY = ZoneInfo("America/New_York")


# =============================
# FUNCTION – GET LIVE QUOTE
# =============================

def get_live_quote():
    url = f"https://financialmodelingprep.com/api/v3/quote/SPY?apikey={FMP_API_KEY}"
    r = requests.get(url)
    data = r.json()
    if not data:
        return None
    q = data[0]

    return {
        "open": q.get("open"),
        "high": q.get("dayHigh"),
        "low": q.get("dayLow"),
        "close": q.get("price"),
        "volume": q.get("volume")
    }


# =============================
# FUNCTION – ROUND TIME TO 5 MIN NYSE CLOCK
# =============================

def rounded_5min_time():
    now_ny = datetime.now(NY)
    rounded = now_ny.replace(
        second=0,
        microsecond=0,
        minute=(now_ny.minute // 5) * 5
    )
    return rounded


# =============================
# MAIN LOGIC – UPSERT LIVE BAR
# =============================

def run_cycle():
    print("Running 5-minute live quote update for SPY...")

    quote = get_live_quote()
    if quote is None:
        print("No quote received.")
        return

    candle_time = rounded_5min_time()

    row = {
        "symbol": "SPY",
        "candle_time": candle_time,
        "open": quote["open"],
        "high": quote["high"],
        "low": quote["low"],
        "close": quote["close"],
        "volume": quote["volume"]
    }

    print("UPSERT ROW:", row)

    supabase.table("saifan_intraday_candles_spy_5m").upsert(row).execute()

    print("Done ✓")


# =============================
# ENTRY POINT
# =============================

if __name__ == "__main__":
    run_cycle()
