import os
import datetime
import requests
from zoneinfo import ZoneInfo
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

FMP_API_KEY = os.getenv("FMP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

FMP_URL = "https://financialmodelingprep.com/api/v3/historical-chart/5min/SPY"
TABLE_NAME = "saifan_intraday_candles_spy_5m"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def round_to_5(dt):
    return dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)


def fetch_fmp():
    url = f"{FMP_URL}?apikey={FMP_API_KEY}"
    r = requests.get(url)
    data = r.json()
    if not isinstance(data, list):
        print("[FMP ERROR]", data)
        return None
    return data


def bar_to_row(bar):
    dt = datetime.datetime.strptime(bar["date"], "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=datetime.timezone.utc)
    dt = round_to_5(dt)

    return {
        "symbol": "SPY",
        "candle_time": dt.isoformat(),
        "open": bar["open"],
        "high": bar["high"],
        "low": bar["low"],
        "close": bar["close"],
        "volume": bar["volume"]
    }


def upsert(row):
    supabase.table(TABLE_NAME).upsert(row, on_conflict="unique_symbol_candle_time").execute()
    print("[UPSERT]", row["candle_time"], row["close"], row["volume"])


def run_cycle():
    print("\n=== SPY WORKER START ===")

    data = fetch_fmp()
    if not data:
        print("[NO DATA]")
        return

    # 1️⃣ עדכון כל ההיסטוריה של היום
    today_us = datetime.datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

    for bar in reversed(data):  # מהישן לחדש
        if bar["date"].startswith(today_us):
            row = bar_to_row(bar)
            upsert(row)

    print("=== HISTORY SYNC COMPLETE ===")

    # 2️⃣ הוספת בר לייב (בר רגעי)
    live_bar = data[0]  # טרי ביותר
    live_row = bar_to_row(live_bar)
    upsert(live_row)

    print("=== LIVE SYNC COMPLETE ===")
    print("=== DONE ===")


if __name__ == "__main__":
    run_cycle()
