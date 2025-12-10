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

    # הכנסת כל ברי היום — כולל הבר האחרון!
    today_us = datetime.datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

    for bar in reversed(data):
        if bar["date"].startswith(today_us):
            upsert(bar_to_row(bar))

    print("=== FULL SYNC COMPLETE ===")
    print("=== DONE ===")


if __name__ == "__main__":
    run_cycle()
