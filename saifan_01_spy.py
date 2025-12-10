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

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HIST_URL = f"https://financialmodelingprep.com/api/v3/historical-chart/5min/SPY?apikey={FMP_API_KEY}"
QUOTE_URL = f"https://financialmodelingprep.com/api/v3/quote/SPY?apikey={FMP_API_KEY}"

# כדי למנוע יצירת LIVE כפול
last_live_candle_time = None


# -------------------------------
# Utility: עיגול זמן ל-5 דקות
# -------------------------------
def round_to_5(dt):
    return dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)


# -------------------------------
# FMP – משיכת היסטוריה רשמית
# -------------------------------
def fetch_history():
    r = requests.get(HIST_URL)
    try:
        data = r.json()
        if isinstance(data, list):
            return data
        return None
    except:
        return None


# -------------------------------
# FMP – משיכת QUOTE (מחיר אחרון)
# -------------------------------
def fetch_quote():
    r = requests.get(QUOTE_URL)
    try:
        data = r.json()
        if isinstance(data, list) and len(data) > 0:
            return data[0]
        return None
    except:
        return None


# -------------------------------
# המרת בר היסטורי לשורה
# -------------------------------
def history_to_row(bar):
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


# -------------------------------
# הכנסה למסד הנתונים (UPSERT)
# -------------------------------
def upsert(row):
    supabase.table(TABLE_NAME).upsert(
        row,
        on_conflict="symbol,candle_time"
    ).execute()
    print("[UPSERT]", row["candle_time"], row["close"], row["volume"])


# -------------------------------
# יצירת בר LIVE חדש כל 5 דקות
# -------------------------------
def build_and_insert_live_bar():
    global last_live_candle_time

    now = datetime.datetime.now(ZoneInfo("America/New_York"))
    rounded = round_to_5(now)

    # למנוע יצירת אותו LIVE פעמיים
    if last_live_candle_time == rounded:
        return

    last_live_candle_time = rounded

    quote = fetch_quote()
    if not quote:
        print("[LIVE] No quote available")
        return

    price = quote.get("price", None)

    # בונים בר חי בסיסי
    live_row = {
        "symbol": "SPY",
        "candle_time": rounded.replace(tzinfo=datetime.timezone.utc).isoformat(),
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": quote.get("volume", 0)
    }

    upsert(live_row)
    print("[LIVE] Inserted LIVE bar at:", rounded)


# -------------------------------
# ריצה מלאה
# -------------------------------
def run_cycle():
    print("\n=== SPY WORKER START ===")

    # 1️⃣ היסטוריה רשמית
    history = fetch_history()
    if history:
        today_us = datetime.datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

        # חשוב: ממיינים מהישן לחדש
        data_sorted = sorted(history, key=lambda x: x["date"])

        count = 0
        for bar in data_sorted:
            if bar["date"].startswith(today_us):
                row = history_to_row(bar)
                upsert(row)
                count += 1

        print(f"=== HISTORY SYNC COMPLETE — {count} rows ===")
    else:
        print("[HISTORY] No history returned.")

    # 2️⃣ בניית בר LIVE תמידי
    build_and_insert_live_bar()

    print("=== DONE ===")


if __name__ == "__main__":
    run_cycle()
