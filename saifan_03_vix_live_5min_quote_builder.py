import os
import requests
from datetime import datetime
from supabase import create_client
from zoneinfo import ZoneInfo

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
FMP_KEY = os.getenv("FMP_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
NY = ZoneInfo("America/New_York")

# Memory for the live bar
live_bar = {
    "open": None,
    "high": None,
    "low": None,
    "close": None,
    "volume": 0
}

current_candle_time = None


def fetch_vix_quote():
    url = f"https://financialmodelingprep.com/api/v3/quote/%5EVIX?apikey={FMP_KEY}"
    r = requests.get(url)
    if r.status_code != 200:
        print("VIX LIVE ERROR:", r.text)
        return None
    data = r.json()
    if not data:
        return None
    return data[0]  # quote record


def rounded_5min(dt):
    return dt.replace(second=0, microsecond=0, minute=(dt.minute // 5) * 5)


def save_bar_to_db(ts):
    row = {
        "symbol": "VIX",
        "candle_time": ts.isoformat(),
        "open": live_bar["open"],
        "high": live_bar["high"],
        "low": live_bar["low"],
        "close": live_bar["close"],
        "volume": live_bar["volume"]
    }

    print("[VIX LIVE] UPSERT:", ts, row)

    supabase.table("saifan_intraday_candles_vix_5m") \
        .upsert(row, on_conflict="symbol,candle_time") \
        .execute()


def start_new_bar(price, ts):
    global live_bar
    global current_candle_time

    current_candle_time = ts
    live_bar = {
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": 0
    }
    print("[VIX LIVE] New bar started:", ts)


def run_vix_cycle():
    global current_candle_time
    global live_bar

    now_ny = datetime.now(NY)
    candle_ts = rounded_5min(now_ny)

    q = fetch_vix_quote()
    if not q:
        return

    price = q.get("price")
    if price is None:
        return

    # If new candle started
    if current_candle_time != candle_ts:
        if current_candle_time is not None:
            save_bar_to_db(current_candle_time)

        start_new_bar(price, candle_ts)
        return

    # Update live bar
    if live_bar["open"] is None:
        start_new_bar(price, candle_ts)

    live_bar["high"] = max(live_bar["high"], price)
    live_bar["low"] = min(live_bar["low"], price)
    live_bar["close"] = price

    print("[VIX LIVE] Update:", candle_ts, live_bar)
