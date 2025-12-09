import os
import requests
from datetime import datetime, timezone
import supabase
import math


# ---------------------------
# Supabase client
# ---------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

supabase_client = supabase.create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "saifan_intraday_candles_spy_5m"
FMP_API_KEY = os.environ.get("FMP_API_KEY")


# ---------------------------
# Indicator calculations
# ---------------------------
def calc_typical_price(o, h, l, c):
    return (h + l + c) / 3.0


def calc_ema(prev_ema, price, period):
    if prev_ema is None:
        return price
    k = 2 / (period + 1)
    return (price - prev_ema) * k + prev_ema


def calc_macd(ema12, ema26):
    if ema12 is None or ema26 is None:
        return None
    return ema12 - ema26


def calc_vwap(cumulative_pv, cumulative_vol):
    if cumulative_vol == 0:
        return None
    return cumulative_pv / cumulative_vol


# ---------------------------
# Fetch last 5-minute bar
# ---------------------------
def fetch_last_5m_bar():
    url = f"https://financialmodelingprep.com/api/v3/historical-chart/5min/SPY?apikey={FMP_API_KEY}"
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if not isinstance(data, list) or len(data) == 0:
            print("[Saifan] ERROR: No bar returned from FMP")
            return None
        return data[0]  # Only the most recent bar
    except Exception as e:
        print(f"[Saifan] ERROR fetching SPY 5m bar: {e}")
        return None


# ---------------------------
# Read last row from DB
# ---------------------------
def get_last_db_row():
    q = (
        supabase_client.table(TABLE_NAME)
        .select("*")
        .order("candle_time", desc=True)
        .limit(1)
    )
    res = q.execute()

    if res.data and len(res.data) > 0:
        return res.data[0]
    return None


# ---------------------------
# Insert or Update SPY Row
# ---------------------------
def db_upsert_spy_row(payload):
    try:
        supabase_client.table(TABLE_NAME).upsert(
            payload,
            on_conflict=["symbol", "candle_time"]
        ).execute()
        print(f"[DB] UPSERT OK — candle_time={payload['candle_time']}")
    except Exception as e:
        print(f"[DB ERROR] {e}")


# ---------------------------
# Build payload & save
# ---------------------------
def run_spy_cycle():
    print("[Saifan] Fetching last real SPY 5m bar...")

    new_bar = fetch_last_5m_bar()
    if not new_bar:
        print("[Saifan] No new bar.")
        return

    # Parse bar
    o = float(new_bar["open"])
    h = float(new_bar["high"])
    l = float(new_bar["low"])
    c = float(new_bar["close"])
    v = float(new_bar["volume"])

    # Convert datetime → UTC ISO format
    dt = datetime.strptime(new_bar["date"], "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone.utc)
    candle_time = dt.isoformat()

    # Pull last DB row for indicators
    last = get_last_db_row()
    if last:
        prev_cpv = last.get("cumulative_pv", 0) or 0
        prev_cvol = last.get("cumulative_vol", 0) or 0
        prev_ema12 = last.get("ema12", None)
        prev_ema26 = last.get("ema26", None)
        prev_macd_signal = last.get("macd_signal", None)
    else:
        prev_cpv = 0
        prev_cvol = 0
        prev_ema12 = None
        prev_ema26 = None
        prev_macd_signal = None

    # Indicators
    typical_price = calc_typical_price(o, h, l, c)

    cumulative_pv = prev_cpv + (typical_price * v)
    cumulative_vol = prev_cvol + v

    ema12 = calc_ema(prev_ema12, c, 12)
    ema26 = calc_ema(prev_ema26, c, 26)
    macd = calc_macd(ema12, ema26)

    if prev_macd_signal is None:
        macd_signal = macd
    else:
        macd_signal = calc_ema(prev_macd_signal, macd, 9)

    macd_hist = macd - macd_signal if macd and macd_signal else None

    vwap = calc_vwap(cumulative_pv, cumulative_vol)

    payload = {
        "symbol": "SPY",
        "candle_time": candle_time,
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": v,
        "typical_price": typical_price,
        "cumulative_pv": cumulative_pv,
        "cumulative_vol": cumulative_vol,
        "ema12": ema12,
        "ema26": ema26,
        "macd": macd,
        "macd_signal": macd_signal,
        "macd_hist": macd_hist,
        "vwap": vwap,
    }

    db_upsert_spy_row(payload)
    print("[Saifan] SPY bar saved.\n")
