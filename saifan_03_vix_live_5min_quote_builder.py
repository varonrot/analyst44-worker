import os
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from supabase import create_client

# ------------------------------------------------------
# Environment
# ------------------------------------------------------
FMP_API_KEY = os.getenv("FMP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
NY = ZoneInfo("America/New_York")

TABLE = "saifan_intraday_candles_vix_5m"


# ------------------------------------------------------
# Fetch LIVE quote for VIX
# ------------------------------------------------------
def get_live_vix():
    url = f"https://financialmodelingprep.com/api/v3/quote/%5EVIX?apikey={FMP_API_KEY}"
    r = requests.get(url)
    if r.status_code != 200:
        print("ERROR fetching VIX live:", r.text)
        return None

    data = r.json()
    if not data:
        return None

    q = data[0]

    return {
        "open": q.get("open"),
        "high": q.get("dayHigh"),
        "low": q.get("dayLow"),
        "close": q.get("price"),
        "volume": q.get("volume"),
    }


# ------------------------------------------------------
# Round NY time to 5-minute candle
# ------------------------------------------------------
def rounded_5min_time():
    now_ny = datetime.now(NY)
    return now_ny.replace(
        second=0,
        microsecond=0,
        minute=(now_ny.minute // 5) * 5
    )


# ------------------------------------------------------
# Main LIVE cycle for VIX
# ------------------------------------------------------
def run_cycle_vix():
    print("=== Saifan 03 – VIX LIVE 5m UPDATE ===")

    quote = get_live_vix()
    if not quote:
        print("No VIX quote returned.")
        return

    candle_time = rounded_5min_time()

    row = {
        "symbol": "VIX",
        "candle_time": candle_time.isoformat(),  # same logic as SPY
        "open": quote["open"],
        "high": quote["high"],
        "low": quote["low"],
        "close": quote["close"],
        "volume": quote["volume"],
    }

    print("UPSERT VIX LIVE bar:", candle_time)

    supabase.table(TABLE).upsert(
        row,
        on_conflict="symbol,candle_time"
    ).execute()

    print("=== VIX LIVE UPDATE COMPLETED ===")


# ------------------------------------------------------
# ENTRY
# ------------------------------------------------------
if __name__ == "__main__":
    run_cycle_vix()
