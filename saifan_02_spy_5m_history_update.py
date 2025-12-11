import os
import requests
from datetime import datetime, date
from supabase import create_client
from zoneinfo import ZoneInfo

# ------------------------------------------------------
# Load environment
# ------------------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
FMP_KEY = os.getenv("FMP_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
NY = ZoneInfo("America/New_York")

# ------------------------------------------------------
# Fetch all 5m bars from FMP
# ------------------------------------------------------
def fetch_spy_history():
    url = f"https://financialmodelingprep.com/api/v3/historical-chart/5min/SPY?apikey={FMP_KEY}"
    r = requests.get(url)
    if r.status_code != 200:
        print("ERROR fetching history:", r.text)
        return None
    return r.json()


# ------------------------------------------------------
# Main logic: overwrite/update all bars from TODAY
# ------------------------------------------------------
def run_history_update():
    print("=== Saifan 02 – FULL SPY 5m DAILY HISTORY UPDATE ===")

    history = fetch_spy_history()
    if not history:
        print("No history returned")
        return

    today_ny = datetime.now(NY).date()

    # round NY time to know which bar is LIVE
    now_ny = datetime.now(NY)
    rounded = now_ny.replace(
        second=0,
        microsecond=0,
        minute=(now_ny.minute // 5) * 5
    )
    print("NY rounded candle:", rounded)

    for bar in history:

        # Parse bar time (already NY time)
        bar_time = datetime.strptime(bar["date"], "%Y-%m-%d %H:%M:%S")

        # Only bars from TODAY
        if bar_time.date() != today_ny:
            continue

        # Do NOT overwrite live bar
        if bar_time >= rounded:
            print("Skipping LIVE bar:", bar_time)
            continue

        row = {
            "symbol": "SPY",
            "candle_time": bar_time.isoformat(),  # stored without timezone
            "open": bar["open"],
            "high": bar["high"],
            "low": bar["low"],
            "close": bar["close"],
            "volume": bar["volume"],
        }

        print("UPSERT official bar:", bar_time)

        supabase.table("saifan_intraday_candles_spy_5m") \
            .upsert(row, on_conflict="symbol,candle_time") \
            .execute()

    print("=== DAILY HISTORY UPDATE COMPLETED ===")


# ------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------
if __name__ == "__main__":
    run_history_update()
