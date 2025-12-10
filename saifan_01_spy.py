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

TABLE_NAME = "saifan_intraday_candles_spy_5m"
FMP_URL = "https://financialmodelingprep.com/api/v3/historical-chart/5min/SPY"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def round_to_5(dt):
    return dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)


def fetch_fmp():
    r = requests.get(f"{FMP_URL}?apikey={FMP_API_KEY}")
    data = r.json()
    if not isinstance(data, list):
        print("[FMP ERROR]", data)
        return None
    return data


def bar_to_row(bar):
    dt = datetime.datetime.strptime(bar["date"], "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=datetime.timezone.utc)

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
    supabase.table(TABLE_NAME).upsert(
        row,
        on_conflict="symbol,candle_time"
    ).execute()
    print("[UPSERT]", row["candle_time"], row["close"], row["volume"])


def run_cycle():
    print("\n=== SPY WORKER START ===")

    data = fetch_fmp()
    if not data:
        print("[NO DATA]")
        return

    # תאריך של היום לפי ניו יורק
    today_us = datetime.datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

    # חשוב: למיין מהישן לחדש כדי שה־UPSERT יעבוד נכון
    data_sorted = sorted(data, key=lambda x: x["date"])

    count = 0

    for bar in data_sorted:
        if bar["date"].startswith(today_us):
            row = bar_to_row(bar)
            upsert(row)
            count += 1

    # --------------------------
    # LIVE BAR (בר חי עד שיהיה היסטורי)
    # --------------------------

    now = datetime.datetime.now(ZoneInfo("America/New_York"))
    rounded = round_to_5(now)

    live_bar = {
        "symbol": "SPY",
        "candle_time": rounded.isoformat(),
        "open": data_sorted[-1]["open"],
        "high": data_sorted[-1]["high"],
        "low": data_sorted[-1]["low"],
        "close": data_sorted[-1]["close"],
        "volume": data_sorted[-1]["volume"],
    }

    upsert(live_bar)
    print("[LIVE] inserted live bar at:", rounded)

    # --------------------------
    print(f"=== FULL SYNC COMPLETE — {count} rows ===")


if __name__ == "__main__":
    run_cycle()
