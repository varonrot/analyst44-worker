import os
import requests
from datetime import datetime
from supabase import create_client
from zoneinfo import ZoneInfo

# ------------------------------------------------------
# Environment
# ------------------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
FMP_KEY = os.getenv("FMP_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
NY = ZoneInfo("America/New_York")

TABLE = "saifan_intraday_candles_vix_5m"


# ------------------------------------------------------
# Fetch 5-minute official VIX bars
# ------------------------------------------------------
def fetch_vix_history():
    # %5EVIX is URL-encoded for ^VIX
    url = f"https://financialmodelingprep.com/api/v3/historical-chart/5min/%5EVIX?apikey={FMP_KEY}"
    r = requests.get(url)

    if r.status_code != 200:
        print("ERROR fetching VIX history:", r.text)
        return None

    return r.json()


# ------------------------------------------------------
# Main logic: overwrite/update all bars from today
# ------------------------------------------------------
def run_vix_history_update():
    print("=== Saifan 04 – FULL VIX 5m DAILY HISTORY UPDATE ===")

    history = fetch_vix_history()
    if not history:
        print("No VIX history returned")
        return

    # Identify today's NY date
    today_ny = datetime.now(NY).date()

    # Identify live bar time (rounded NY time)
    now_ny = datetime.now(NY)
    rounded = now_ny.replace(
        second=0,
        microsecond=0,
        minute=(now_ny.minute // 5) * 5
    )
    print("NY rounded candle:", rounded)

    for bar in history:

        # FMP VIX bar timestamp (already NY time)
        bar_time = datetime.strptime(bar["date"], "%Y-%m-%d %H:%M:%S")

        # Only today's bars
        if bar_time.date() != today_ny:
            continue

        # Do NOT overwrite the active live bar
        if bar_time == rounded:
            print("Skipping the current LIVE VIX bar:", bar_time)
            continue

        row = {
            "symbol": "VIX",
            "candle_time": bar_time.isoformat(),  # same format as SPY
            "open": bar["open"],
            "high": bar["high"],
            "low": bar["low"],
            "close": bar["close"],
            "volume": bar["volume"],
        }

        print("UPSERT official VIX bar:", bar_time)

        supabase.table(TABLE) \
            .upsert(row, on_conflict="symbol,candle_time") \
            .execute()

    print("=== VIX DAILY HISTORY UPDATE COMPLETED ===")


# ------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------
if __name__ == "__main__":
    run_vix_history_update()
