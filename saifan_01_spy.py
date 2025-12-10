import os
import requests
import datetime
from zoneinfo import ZoneInfo
from supabase import create_client, Client

# -------------------------------------------
# CONFIG
# -------------------------------------------
API_KEY = os.getenv("FMP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

TABLE = "saifan_intraday_candles_spy_5m"
SYMBOL = "SPY"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -------------------------------------------
# HELPERS
# -------------------------------------------

def round_to_5(dt: datetime.datetime):
    """מעגל זמן ל-5 דקות"""
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)


def upsert(row):
    """UPSERT בסיסי לפי candle_time"""
    supabase.table(TABLE).upsert(
        row,
        on_conflict="candle_time"
    ).execute()

    print("[UPSERT]", row["candle_time"], row["close"], row["volume"])


# -------------------------------------------
# LIVE BAR
# -------------------------------------------

def fetch_live_quote():
    """שליפת ציטוט חי"""
    url = f"https://financialmodelingprep.com/api/v3/quote/{SYMBOL}?apikey={API_KEY}"
    r = requests.get(url)
    data = r.json()
    if not data:
        return None
    return data[0]


def build_live_bar():
    """בניית BAR לייב מ-QUOTE"""
    q = fetch_live_quote()
    if not q:
        print("[LIVE] No quote data")
        return None

    now_ny = datetime.datetime.now(ZoneInfo("America/New_York"))
    rounded = round_to_5(now_ny)

    row = {
        "candle_time": rounded.isoformat(),
        "open": q["previousClose"],
        "high": q["price"],
        "low": q["price"],
        "close": q["price"],
        "volume": q["volume"]
    }

    print("[LIVE BAR]", row)
    return row


# -------------------------------------------
# HISTORICAL DATA (OFFICIAL)
# -------------------------------------------

def fetch_hist_5m():
    """היסטוריה רשמית 5 דקות"""
    url = f"https://financialmodelingprep.com/api/v3/historical-chart/5min/{SYMBOL}?apikey={API_KEY}"
    r = requests.get(url)
    data = r.json()
    return data


def build_hist_bar(bar):
    """המרת בר מהיסטוריה לטבלה שלנו"""
    dt = datetime.datetime.fromisoformat(bar["date"].replace("Z", "+00:00"))
    return {
        "candle_time": dt.isoformat(),
        "open": bar["open"],
        "high": bar["high"],
        "low": bar["low"],
        "close": bar["close"],
        "volume": bar["volume"]
    }


# -------------------------------------------
# MAIN LOGIC
# -------------------------------------------

def run_cycle():
    print("=== SPY WORKER START ===")

    # ----------- 1) LIVE BAR -----------
    live = build_live_bar()
    if live:
        upsert(live)

    # ----------- 2) HISTORICAL -----------
    hist = fetch_hist_5m()
    if not hist:
        print("[HIST] no data")
        return

    today_ny = datetime.datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

    # historical מגיע הפוך → נמיין לפי זמן
    hist_sorted = sorted(hist, key=lambda x: x["date"])

    count = 0
    for b in hist_sorted:
        if b["date"].startswith(today_ny):
            row = build_hist_bar(b)
            upsert(row)
            count += 1

    print(f"=== HIST COMPLETE — {count} bars ===")


# -------------------------------------------
# ENTRY
# -------------------------------------------

if __name__ == "__main__":
    run_cycle()
