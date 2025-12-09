import os
import requests
import datetime
from supabase import create_client, Client


# ==============================
# CONFIG
# ==============================

API_KEY = os.getenv("FMP_API_KEY")
FMP_URL = "https://financialmodelingprep.com/api/v3/historical-chart/5min/SPY"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

TABLE_NAME = "saifan_intraday_candles_spy_5m"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ==============================
# FETCH LAST 5m BAR
# ==============================

def fetch_last_spy_bar():
    try:
        url = f"{FMP_URL}?apikey={API_KEY}"
        resp = requests.get(url, timeout=10)

        if resp.status_code != 200:
            print("[FMP] Error:", resp.text)
            return None

        data = resp.json()
        if not data or len(data) == 0:
            print("[FMP] No data returned")
            return None

        # הבארים ב-FMP מגיעים מהחדש לישן – לוקחים את הראשון
        last = data[0]

        candle_time = datetime.datetime.strptime(last["date"], "%Y-%m-%d %H:%M:%S")

        return {
            "symbol": "SPY",
            "candle_time": candle_time.isoformat(),
            "open": last["open"],
            "high": last["high"],
            "low": last["low"],
            "close": last["close"],
            "volume": last["volume"]
        }

    except Exception as e:
        print("[FMP] Failed to fetch last bar:", e)
        return None


# ==============================
# INDICATORS
# ==============================

def calculate_indicators(row):
    """
    חישוב EMA12/26 + MACD + VWAP לשורה בודדת.
    VWAP מחושב על typ_price * volume.
    """
    close_price = row["close"]
    volume = row["volume"]

    typical_price = (row["high"] + row["low"] + row["close"]) / 3
    cumulative_pv = typical_price * volume
    cumulative_vol = volume
    vwap = cumulative_pv / cumulative_vol if cumulative_vol != 0 else None

    # EMA12 & EMA26 (בר ראשון = מחיר סגירה)
    ema12 = close_price
    ema26 = close_price

    macd = ema12 - ema26
    macd_signal = macd  # בבר ראשון = MACD
    macd_hist = macd - macd_signal

    row["ema12"] = ema12
    row["ema26"] = ema26
    row["macd"] = macd
    row["macd_signal"] = macd_signal
    row["macd_hist"] = macd_hist

    row["typical_price"] = typical_price
    row["cumulative_pv"] = cumulative_pv
    row["cumulative_vol"] = cumulative_vol
    row["vwap"] = vwap

    return row


# ==============================
# UPSERT TO DB
# ==============================

def db_upsert_spy_row(payload: dict):
    try:
        supabase.table(TABLE_NAME).upsert(payload, ignore_duplicates=False).execute()
        print("[DB] UPSERT OK:", payload["candle_time"])
        return True
    except Exception as e:
        print("[DB] UPSERT FAILED:", e)
        return False


# ==============================
# MAIN CYCLE
# ==============================

def run_spy_cycle():
    print("=== [SPY] Fetching latest 5m bar ===")

    row = fetch_last_spy_bar()
    if not row:
        print("[SPY] No row to process.")
        return

    # Check if row already exists (avoid duplicates)
    existing = (
        supabase.table(TABLE_NAME)
        .select("id")
        .eq("symbol", "SPY")
        .eq("candle_time", row["candle_time"])
        .execute()
    )

    if existing.data:
        print("[DB] Row already exists. Skipping:", row["candle_time"])
        return

    # Indicators
    row = calculate_indicators(row)

    # Upsert
    db_upsert_spy_row(row)



# ==============================
# TEST (LOCAL)
# ==============================

if __name__ == "__main__":
    run_spy_cycle()
